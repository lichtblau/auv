/* auv.c -- the whole C surface of src/Uv.
 *
 * WHY THIS FILE EXISTS. Three things libuv needs and Ante cannot express:
 *
 *   * CALLBACKS. Every uv operation completes into a C function pointer. Ante has no
 *     export story and its closures cannot cross an FFI boundary, so every completion
 *     lands in a trampoline here.
 *   * STRUCT LAYOUT. uv_loop_t, uv_timer_t, uv_process_options_t and friends are
 *     by-value aggregates with platform-dependent layout. None of it crosses the
 *     boundary: Ante sees `Ptr U8` and the sizes come from uv_handle_size/uv_req_size.
 *   * RESUMING A COROUTINE FROM A CALLBACK. A completion has to wake the task that is
 *     parked on it. Storing bytes into a wait slot and calling mco_coro_resume needs no
 *     Ante code at all, which is the point: no Ante closure is ever reachable from libuv.
 *
 * THE WAIT SLOT is the whole protocol. A task registers a uv operation whose handle (or
 * request) carries a slot pointer in its ->data, then parks on that slot. The trampoline
 * writes the completion into the slot and, if a task was parked, resumes it. The slot
 * carries a one-shot latch so two completions racing for one waiter cannot double-resume
 * (a double mco_coro_resume aborts the process).
 *
 * EVERY resume of a task funnels through auv__resume, which also maintains the
 * "task currently running" pointer the Ante side reads to identify itself. There is
 * exactly one call to mco_coro_resume in this module and none anywhere else.
 *
 * Threading: libuv's threadpool (fs, dns) completes on the LOOP thread, never on a worker.
 * Every trampoline checks that anyway (see auv__check_thread) -- a coroutine touched from
 * a worker thread would corrupt the switch bookkeeping in ways no test would survive.
 */

#include <uv.h>

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <dirent.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "minicoro.h"

/* aminicoro's own thin wrappers live in minicoro.c, not in the header. This is the one
 * the trampolines need; a failed resume (a coroutine resumed twice, or resumed while it
 * is running) aborts inside it, which is exactly the diagnosis we want. */
char mco_coro_resume(mco_coro *k);

/* --- wait slots ------------------------------------------------------------------- */

/* Slot states. The transitions are the latch:
 *     EMPTY     -> PARKED     a task registered itself (auv_slot_park)
 *     EMPTY     -> DONE       a completion arrived before anyone parked
 *     PARKED    -> DONE       a completion arrived and woke the parked task
 *     DONE      -> DELIVERED  the task read the completion (auv_slot_consume)
 *     DELIVERED -> EMPTY      the slot was rearmed for the next operation
 * A fill into DONE or DELIVERED is dropped. That is what makes a losing racer harmless. */
#define AUV_EMPTY     0
#define AUV_PARKED    1
#define AUV_DONE      2
#define AUV_DELIVERED 3

typedef struct auv_slot {
    int32_t state;
    int32_t kind;
    void   *task;      /* mco_coro* to resume; set at park time, cleared on fill */
    int64_t status;    /* uv status / signum / fs result; negative is a uv error code */
    int64_t aux;       /* nread / event mask / term signal -- per-kind meaning */
    void   *payload;   /* malloc'd result buffer; the slot owns it until taken */
} auv_slot;

_Static_assert(sizeof(void *) == 8, "auv assumes 64-bit pointers");
_Static_assert(sizeof(auv_slot) == 40, "auv_slot layout changed");
_Static_assert(offsetof(auv_slot, state) == 0, "auv_slot.state moved");
_Static_assert(offsetof(auv_slot, kind) == 4, "auv_slot.kind moved");
_Static_assert(offsetof(auv_slot, task) == 8, "auv_slot.task moved");
_Static_assert(offsetof(auv_slot, status) == 16, "auv_slot.status moved");
_Static_assert(offsetof(auv_slot, aux) == 24, "auv_slot.aux moved");
_Static_assert(offsetof(auv_slot, payload) == 32, "auv_slot.payload moved");

/* Completion kinds. Kept in sync with Uv.an's k_* accessors. */
#define AUV_K_NONE       0
#define AUV_K_TIMER      1
#define AUV_K_POLL       2
#define AUV_K_SIGNAL     3
#define AUV_K_PREPARE    4
#define AUV_K_CHECK      5
#define AUV_K_IDLE       6
#define AUV_K_ASYNC      7
#define AUV_K_CLOSED     8
#define AUV_K_CANCELLED  9
#define AUV_K_JOINED    10
#define AUV_K_CONNECT   11
#define AUV_K_CONNECTION 12
#define AUV_K_READ      13
#define AUV_K_WRITE     14
#define AUV_K_SHUTDOWN  15
#define AUV_K_EOF       16
#define AUV_K_FS        17
#define AUV_K_ADDRINFO  18
#define AUV_K_EXIT      19
#define AUV_K_DGRAM     20
#define AUV_K_FSEVENT   21

/* --- the one resume site ------------------------------------------------------------ */

/* The task whose coroutine is currently running, or NULL on the scheduler's own stack.
 * The Ante side reads this to answer "which task am I?" without depending on
 * mco_coro_running, which reports whatever coroutine an intermediate effect handler
 * happens to have interposed. */
static void *auv__current_task = NULL;

void *auv_current_task(void) { return auv__current_task; }

static void auv__resume(void *task) {
    void *prev = auv__current_task;
    auv__current_task = task;
    mco_coro_resume(task);
    auv__current_task = prev;
}

/* Resume a task from the scheduler's ready-queue drain. */
void auv_resume_task(void *task) {
    if (task)
        auv__resume(task);
}

/* --- slot API ----------------------------------------------------------------------- */

auv_slot *auv_slot_new(void) {
    auv_slot *s = calloc(1, sizeof(auv_slot));
    return s;
}

void auv_slot_free(auv_slot *s) {
    if (!s)
        return;
    assert(s->state != AUV_PARKED && "freeing a slot a task is parked on");
    free(s->payload);
    free(s);
}

int32_t auv_slot_state(auv_slot *s)   { return s ? s->state : AUV_EMPTY; }
int32_t auv_slot_kind(auv_slot *s)    { return s ? s->kind : AUV_K_NONE; }
int64_t auv_slot_status(auv_slot *s)  { return s ? s->status : 0; }
int64_t auv_slot_aux(auv_slot *s)     { return s ? s->aux : 0; }

/* Hand the payload to the caller, who owns it from here (auv_buf_free releases it). */
void *auv_slot_take_payload(auv_slot *s) {
    if (!s)
        return NULL;
    void *p = s->payload;
    s->payload = NULL;
    return p;
}

/* Ready the slot for the next operation. Dropping a completion that arrived after the
 * previous one was consumed is deliberate: the operation it belonged to is over. */
void auv_slot_rearm(auv_slot *s) {
    if (!s)
        return;
    assert(s->state != AUV_PARKED && "rearming a slot a task is parked on");
    free(s->payload);
    s->payload = NULL;
    s->kind = AUV_K_NONE;
    s->status = 0;
    s->aux = 0;
    s->state = AUV_EMPTY;
}

/* Returns 1 when the caller must suspend, 0 when a completion is already waiting. */
int32_t auv_slot_park(auv_slot *s, void *task) {
    assert(s && "parking on a null slot");
    assert(s->state != AUV_PARKED && "two tasks parked on one slot");
    if (s->state == AUV_DONE)
        return 0;
    s->state = AUV_PARKED;
    s->task = task;
    return 1;
}

/* Move the completion out of the slot; the fields stay readable until the next rearm. */
void auv_slot_consume(auv_slot *s) {
    if (!s)
        return;
    assert(s->state == AUV_DONE && "consuming a slot with no completion");
    s->state = AUV_DELIVERED;
    s->task = NULL;
}

/* The single writer. Returns the task to resume, or NULL when there is none (either
 * nobody was parked, or the latch dropped this completion). */
static void *auv__slot_fill(auv_slot *s, int32_t kind, int64_t status, int64_t aux, void *payload) {
    if (!s)
        return NULL;
    if (s->state != AUV_EMPTY && s->state != AUV_PARKED) {
        free(payload);            /* the loser of a race owns nothing */
        return NULL;
    }
    free(s->payload);
    s->payload = payload;
    s->kind = kind;
    s->status = status;
    s->aux = aux;
    void *task = (s->state == AUV_PARKED) ? s->task : NULL;
    s->task = NULL;
    s->state = AUV_DONE;
    return task;
}

/* Fill and resume in place. This is what a trampoline calls: the woken task runs to its
 * next park (or to completion) inside the loop turn that produced the completion. */
void auv_slot_deliver(auv_slot *s, int32_t kind, int64_t status, int64_t aux, void *payload) {
    void *task = auv__slot_fill(s, kind, status, aux, payload);
    if (task)
        auv__resume(task);
}

/* Fill without resuming. Returns 1 when a parked task now needs scheduling. Callers on a
 * task's stack use this so a wake never nests one task's execution inside another's. */
int32_t auv_slot_post(auv_slot *s, int32_t kind, int64_t status, int64_t aux, void *payload) {
    return auv__slot_fill(s, kind, status, aux, payload) != NULL;
}

void auv_buf_free(void *p) { free(p); }

/* Copy `len` bytes out of a payload buffer. */
void auv_buf_read(const void *src, size_t off, void *dst, size_t len) {
    if (src && dst && len)
        memcpy(dst, (const char *)src + off, len);
}

void *auv_buf_alloc(size_t len) { return malloc(len ? len : 1); }

void auv_buf_write(void *dst, size_t off, const void *src, size_t len) {
    if (src && dst && len)
        memcpy((char *)dst + off, src, len);
}

/* --- errors and strings -------------------------------------------------------------- */

/* Copy a NUL-terminated C string into `buf`, truncating to `cap`. Returns the full length,
 * so a caller sizes its buffer with a first call passing cap == 0. */
static size_t auv__copy_cstr_n(const char *s, size_t n, char *buf, size_t cap) {
    if (!s)
        return 0;
    if (buf && cap) {
        size_t k = n < cap ? n : cap;
        memcpy(buf, s, k);
    }
    return n;
}

static size_t auv__copy_cstr(const char *s, char *buf, size_t cap) {
    return s ? auv__copy_cstr_n(s, strlen(s), buf, cap) : 0;
}

size_t auv_err_name(int32_t code, char *buf, size_t cap) {
    return auv__copy_cstr(uv_err_name(code), buf, cap);
}

size_t auv_err_msg(int32_t code, char *buf, size_t cap) {
    return auv__copy_cstr(uv_strerror(code), buf, cap);
}

size_t auv_cstr_copy(const char *s, char *buf, size_t cap) {
    return auv__copy_cstr(s, buf, cap);
}

/* Ante strings are length-counted and not necessarily NUL-terminated; anything libuv reads
 * as a C string is copied through here first. Released with auv_buf_free. */
char *auv_cstr_new(const void *bytes, size_t n) {
    char *p = malloc(n + 1);
    if (!p)
        return NULL;
    if (n)
        memcpy(p, bytes, n);
    p[n] = '\0';
    return p;
}

int64_t auv_hrtime_ms(void) { return (int64_t)(uv_hrtime() / 1000000ull); }

/* Block the calling thread. The synchronous stand-in for a timer, for programs run without
 * a loop at all. */
void auv_sleep_ms(int64_t ms) { uv_sleep(ms < 0 ? 0 : (unsigned int)ms); }

/* --- raw file descriptors ------------------------------------------------------------- */

/* libuv can adopt an existing descriptor (uv_poll_init, uv_pipe_open), so the binding needs
 * a way to make and drive one. These are also what the module's tests build their fixtures
 * out of; Ante has no POSIX layer of its own. Descriptor pairs come back through a caller
 * buffer of two int32_t, read with auv_i32_at. */

int32_t auv_i32_at(const void *p, size_t i) { return ((const int32_t *)p)[i]; }
void    auv_i32_set(void *p, size_t i, int32_t v) { ((int32_t *)p)[i] = v; }

int32_t auv_pipe_fds(int32_t *fds) {
    int raw[2];
    if (pipe(raw) != 0)
        return -errno;
    fds[0] = raw[0];
    fds[1] = raw[1];
    return 0;
}

int32_t auv_socketpair_fds(int32_t *fds) {
    int raw[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, raw) != 0)
        return -errno;
    fds[0] = raw[0];
    fds[1] = raw[1];
    return 0;
}

int32_t auv_fd_nonblock(int32_t fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -errno;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0 ? -errno : 0;
}

int64_t auv_fd_write(int32_t fd, const void *buf, size_t n) {
    ssize_t r = write(fd, buf, n);
    return r < 0 ? -errno : (int64_t)r;
}

int64_t auv_fd_read(int32_t fd, void *buf, size_t n) {
    ssize_t r = read(fd, buf, n);
    return r < 0 ? -errno : (int64_t)r;
}

int32_t auv_fd_close(int32_t fd) { return close(fd) < 0 ? -errno : 0; }

int32_t auv_getpid(void) { return (int32_t)getpid(); }

int32_t auv_kill(int32_t pid, int32_t sig) { return kill((pid_t)pid, sig) < 0 ? -errno : 0; }

/* Signal numbers differ across platforms, so they are read rather than hardcoded. */
int32_t auv_sigusr1(void) { return SIGUSR1; }
int32_t auv_sigusr2(void) { return SIGUSR2; }
int32_t auv_sigterm(void) { return SIGTERM; }
int32_t auv_sigint(void)  { return SIGINT; }

/* --- loop --------------------------------------------------------------------------- */

typedef struct auv_loop_ctx {
    uv_thread_t thread;        /* whoever called auv_loop_new */
    int         running;       /* inside uv_run: a nested run is refused */
    int64_t     thread_faults; /* trampolines that ran off the loop thread */
    int64_t     read_stops;    /* uv_read_stop calls, for the backpressure test */
} auv_loop_ctx;

static auv_loop_ctx *auv__ctx(uv_loop_t *loop) {
    return loop ? (auv_loop_ctx *)uv_loop_get_data(loop) : NULL;
}

/* Note: creating a loop does NOT ignore SIGPIPE -- measured, 2026-07-29: the disposition is
 * SIG_DFL both before and after. An embedder that writes to descriptors whose peer can vanish
 * installs SIG_IGN itself; nothing here does it for them. */
uv_loop_t *auv_loop_new(void) {
    uv_loop_t *loop = malloc(uv_loop_size());
    if (!loop)
        return NULL;
    if (uv_loop_init(loop) != 0) {
        free(loop);
        return NULL;
    }
    auv_loop_ctx *ctx = calloc(1, sizeof(auv_loop_ctx));
    if (!ctx) {
        uv_loop_close(loop);
        free(loop);
        return NULL;
    }
    ctx->thread = uv_thread_self();
    uv_loop_set_data(loop, ctx);
    return loop;
}

static int auv__run(uv_loop_t *loop, uv_run_mode mode) {
    auv_loop_ctx *ctx = auv__ctx(loop);
    assert(ctx && "running a loop that was never initialized");
    assert(!ctx->running && "re-entrant uv_run");
    ctx->running = 1;
    int r = uv_run(loop, mode);
    ctx->running = 0;
    return r;
}

int32_t auv_loop_run_once(uv_loop_t *loop)   { return auv__run(loop, UV_RUN_ONCE); }
int32_t auv_loop_run_nowait(uv_loop_t *loop) { return auv__run(loop, UV_RUN_NOWAIT); }
int32_t auv_loop_alive(uv_loop_t *loop)      { return loop ? uv_loop_alive(loop) : 0; }

/* The loop's own clock, in milliseconds: read ONCE per turn and cached, which is what makes
 * every timer in a turn agree about what time it is. A long callback therefore sees a clock
 * that has not moved, and uv_update_time is how a program that cares says "look again". */
int64_t auv_loop_now(uv_loop_t *loop) { return loop ? (int64_t)uv_now(loop) : 0; }
void auv_loop_update_time(uv_loop_t *loop) { if (loop) uv_update_time(loop); }

/* What you would need to drive this loop from inside another one -- a GUI toolkit's, say:
 * poll the backend descriptor, and when it fires (or the timeout expires) run one non-blocking
 * turn. -1 from the timeout means "no timeout", i.e. block until something happens. */
int32_t auv_loop_backend_fd(uv_loop_t *loop) { return loop ? uv_backend_fd(loop) : -1; }
int32_t auv_loop_backend_timeout(uv_loop_t *loop) { return loop ? uv_backend_timeout(loop) : 0; }

/* Whether the handle owns its slot is recorded in the LOW BIT of the data pointer, not in
 * the slot. A handle that borrows a task's slot can still be mid-close when that task is
 * reaped and its slot freed, and the close callback must be able to tell the two cases apart
 * without dereferencing what may already be gone. */
#define AUV_SLOT_OWNED 1u

static auv_slot *auv__slot_of(const uv_handle_t *h) {
    uintptr_t d = (uintptr_t)uv_handle_get_data(h);
    return (auv_slot *)(d & ~(uintptr_t)AUV_SLOT_OWNED);
}

static void auv__on_close(uv_handle_t *h) {
    uintptr_t d = (uintptr_t)uv_handle_get_data(h);
    if (d & AUV_SLOT_OWNED) {
        auv_slot *s = (auv_slot *)(d & ~(uintptr_t)AUV_SLOT_OWNED);
        assert(s->state != AUV_PARKED && "closing a handle with a task still parked on it");
        free(s->payload);
        free(s);
    }
    free(h);
}

static void auv__walk_close(uv_handle_t *h, void *unused) {
    (void)unused;
    if (!uv_is_closing(h))
        uv_close(h, auv__on_close);
}

void auv_loop_walk_close_all(uv_loop_t *loop) {
    if (loop)
        uv_walk(loop, auv__walk_close, NULL);
}

/* uv_loop_close, and on success the loop memory too. UV_EBUSY means handles are still
 * closing: the caller drains with auv_loop_run_nowait and retries. */
int32_t auv_loop_close_free(uv_loop_t *loop) {
    if (!loop)
        return 0;
    int r = uv_loop_close(loop);
    if (r != 0)
        return r;
    free(auv__ctx(loop));
    free(loop);
    return 0;
}

int64_t auv_loop_thread_faults(uv_loop_t *loop) {
    auv_loop_ctx *ctx = auv__ctx(loop);
    return ctx ? ctx->thread_faults : 0;
}

int64_t auv_loop_read_stops(uv_loop_t *loop) {
    auv_loop_ctx *ctx = auv__ctx(loop);
    return ctx ? ctx->read_stops : 0;
}

/* Every trampoline runs this. A completion delivered from a threadpool worker would
 * resume a coroutine on the wrong stack; counting the fault keeps the proof cheap in
 * release builds and the assert makes it loud in debug ones. */
static void auv__check_thread(uv_loop_t *loop) {
    auv_loop_ctx *ctx = auv__ctx(loop);
    if (!ctx)
        return;
    uv_thread_t self = uv_thread_self();
    if (!uv_thread_equal(&self, &ctx->thread)) {
        ctx->thread_faults++;
        assert(0 && "uv completion delivered off the loop thread");
    }
}

/* --- handles ------------------------------------------------------------------------ */

static uv_handle_t *auv__alloc_handle(uv_handle_type type) {
    size_t n = uv_handle_size(type);
    uv_handle_t *h = calloc(1, n);
    return h;
}

auv_slot *auv_handle_slot(uv_handle_t *h) { return h ? auv__slot_of(h) : NULL; }

void auv_handle_set_slot(uv_handle_t *h, auv_slot *s) {
    if (h)
        uv_handle_set_data(h, s);
}

/* Attach a slot AND hand the handle ownership of it: the close callback frees it, on a later
 * loop turn than any callback that might still be reading it. This is what a long-lived
 * resource uses; a transient handle borrowing its owner's slot uses auv_handle_set_slot. */
void auv_handle_adopt_slot(uv_handle_t *h, auv_slot *s) {
    if (!h)
        return;
    uv_handle_set_data(h, (void *)((uintptr_t)s | (s ? AUV_SLOT_OWNED : 0u)));
}

int32_t auv_handle_is_closing(uv_handle_t *h) { return h ? uv_is_closing(h) : 1; }

/* Whether a handle COUNTS. uv_run returns when nothing referenced is left to wait for, so an
 * unreferenced handle is watched exactly as before and simply stops being a reason to keep
 * running -- which is the difference between a background watcher and a job. A heartbeat timer
 * or a SIGINT watch is the whole use: without this, parking on one makes uv_run never return.
 * Orthogonal to active/inactive: unreferencing an idle handle changes nothing until it starts. */
void auv_ref(uv_handle_t *h)   { if (h) uv_ref(h); }
void auv_unref(uv_handle_t *h) { if (h) uv_unref(h); }
int32_t auv_has_ref(uv_handle_t *h) { return h ? uv_has_ref(h) : 0; }

void auv_close(uv_handle_t *h) {
    if (h && !uv_is_closing(h))
        uv_close(h, auv__on_close);
}

/* Wake whoever is parked on this handle BEFORE the handle starts closing, so a reader
 * never sees its completion disappear. Idempotent through the latch. */
void auv_close_waking(uv_handle_t *h, int32_t kind, int64_t status) {
    if (!h)
        return;
    auv_slot *s = auv_handle_slot(h);
    if (s)
        auv_slot_deliver(s, kind, status, 0, NULL);
    auv_close(h);
}

/* --- timer -------------------------------------------------------------------------- */

static void auv__on_timer(uv_timer_t *h) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_TIMER, 0, 0, NULL);
}

uv_handle_t *auv_timer_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_TIMER);
    if (!h)
        return NULL;
    if (uv_timer_init(loop, (uv_timer_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_timer_start(uv_handle_t *h, int64_t timeout_ms, int64_t repeat_ms) {
    return uv_timer_start((uv_timer_t *)h, auv__on_timer,
                          (uint64_t)timeout_ms, (uint64_t)repeat_ms);
}

int32_t auv_timer_stop(uv_handle_t *h) { return uv_timer_stop((uv_timer_t *)h); }

/* --- poll --------------------------------------------------------------------------- */

static void auv__on_poll(uv_poll_t *h, int status, int events) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_POLL, status, events, NULL);
}

uv_handle_t *auv_poll_new(uv_loop_t *loop, int32_t fd) {
    uv_handle_t *h = auv__alloc_handle(UV_POLL);
    if (!h)
        return NULL;
    if (uv_poll_init(loop, (uv_poll_t *)h, fd) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_poll_start(uv_handle_t *h, int32_t events) {
    return uv_poll_start((uv_poll_t *)h, events, auv__on_poll);
}

int32_t auv_poll_stop(uv_handle_t *h) { return uv_poll_stop((uv_poll_t *)h); }

/* --- signal ------------------------------------------------------------------------- */

static void auv__on_signal(uv_signal_t *h, int signum) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_SIGNAL, signum, 0, NULL);
}

uv_handle_t *auv_signal_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_SIGNAL);
    if (!h)
        return NULL;
    if (uv_signal_init(loop, (uv_signal_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_signal_start(uv_handle_t *h, int32_t signum) {
    return uv_signal_start((uv_signal_t *)h, auv__on_signal, signum);
}

int32_t auv_signal_stop(uv_handle_t *h) { return uv_signal_stop((uv_signal_t *)h); }

/* --- prepare / check / idle ---------------------------------------------------------- */

static void auv__on_prepare(uv_prepare_t *h) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_PREPARE, 0, 0, NULL);
}

static void auv__on_check(uv_check_t *h) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_CHECK, 0, 0, NULL);
}

static void auv__on_idle(uv_idle_t *h) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_IDLE, 0, 0, NULL);
}

uv_handle_t *auv_prepare_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_PREPARE);
    if (!h)
        return NULL;
    if (uv_prepare_init(loop, (uv_prepare_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_prepare_start(uv_handle_t *h) { return uv_prepare_start((uv_prepare_t *)h, auv__on_prepare); }
int32_t auv_prepare_stop(uv_handle_t *h)  { return uv_prepare_stop((uv_prepare_t *)h); }

uv_handle_t *auv_check_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_CHECK);
    if (!h)
        return NULL;
    if (uv_check_init(loop, (uv_check_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_check_start(uv_handle_t *h) { return uv_check_start((uv_check_t *)h, auv__on_check); }
int32_t auv_check_stop(uv_handle_t *h)  { return uv_check_stop((uv_check_t *)h); }

uv_handle_t *auv_idle_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_IDLE);
    if (!h)
        return NULL;
    if (uv_idle_init(loop, (uv_idle_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_idle_start(uv_handle_t *h) { return uv_idle_start((uv_idle_t *)h, auv__on_idle); }
int32_t auv_idle_stop(uv_handle_t *h)  { return uv_idle_stop((uv_idle_t *)h); }

/* --- async wakeups -------------------------------------------------------------------- */

static void auv__on_async(uv_async_t *h) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_ASYNC, 0, 0, NULL);
}

uv_handle_t *auv_async_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_ASYNC);
    if (!h)
        return NULL;
    if (uv_async_init(loop, (uv_async_t *)h, auv__on_async) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_async_send(uv_handle_t *h) { return uv_async_send((uv_async_t *)h); }

/* --- streams --------------------------------------------------------------------------- */

/* Requests carry their own slot and, for writes, the buffer they are sending. A pending
 * request keeps its buffer alive until its callback fires, which is the whole rule: libuv
 * borrows the memory and says nothing about it until then. */
typedef struct auv_wreq {
    uv_write_t req;
    uv_buf_t   buf;
    auv_slot  *slot;
} auv_wreq;

typedef struct auv_creq {
    uv_connect_t req;
    auv_slot    *slot;
} auv_creq;

typedef struct auv_sreq {
    uv_shutdown_t req;
    auv_slot     *slot;
} auv_sreq;

uv_handle_t *auv_tcp_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_TCP);
    if (!h)
        return NULL;
    if (uv_tcp_init(loop, (uv_tcp_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

uv_handle_t *auv_pipe_new(uv_loop_t *loop, int32_t ipc) {
    uv_handle_t *h = auv__alloc_handle(UV_NAMED_PIPE);
    if (!h)
        return NULL;
    if (uv_pipe_init(loop, (uv_pipe_t *)h, ipc) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

/* Adopt an existing descriptor. This is the bridge for anything already open: a pty master,
 * a socketpair end, a child's stdio. */
int32_t auv_pipe_open(uv_handle_t *h, int32_t fd) {
    return uv_pipe_open((uv_pipe_t *)h, fd);
}

/* Parse a NUMERIC address, either family, into `ss`. libuv has one parser per family and each
 * rejects the other's notation, so trying both is what makes a single Ante `connect ip port`
 * work for "127.0.0.1" and for "::1" alike -- and it is the family of the parsed address that
 * decides which socket uv_tcp_bind/uv_tcp_connect then creates.
 *
 * That this was v4-only is what made getaddrinfo's own answers unusable: `resolve` reports v6
 * addresses through uv_ip6_name, and the caller could not hand one back. Neither parser looks
 * anything up -- a name gets UV_EINVAL from both, and resolving is `resolve`'s job. */
static int auv__parse_addr(const char *ip, int32_t port, struct sockaddr_storage *ss) {
    memset(ss, 0, sizeof(*ss));
    if (uv_ip4_addr(ip, port, (struct sockaddr_in *)ss) == 0)
        return 0;
    return uv_ip6_addr(ip, port, (struct sockaddr_in6 *)ss);
}

int32_t auv_tcp_bind(uv_handle_t *h, const char *ip, int32_t port) {
    struct sockaddr_storage ss;
    int r = auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    return uv_tcp_bind((uv_tcp_t *)h, (const struct sockaddr *)&ss, 0);
}

/* The two families keep the port in differently named fields of differently shaped structs,
 * hence the two arms. -1 for anything else, a unix-domain socket included: it has no port and
 * saying 0 would be a lie. */
static int32_t auv__port_of(const struct sockaddr_storage *ss) {
    if (ss->ss_family == AF_INET)
        return (int32_t)ntohs(((const struct sockaddr_in *)ss)->sin_port);
    if (ss->ss_family == AF_INET6)
        return (int32_t)ntohs(((const struct sockaddr_in6 *)ss)->sin6_port);
    return -1;
}

/* The numeric address as text, either family, into a caller buffer. */
static size_t auv__addr_text(const struct sockaddr *addr, char *out, size_t cap) {
    if (!addr || cap == 0)
        return 0;
    out[0] = '\0';
    if (addr->sa_family == AF_INET)
        uv_ip4_name((const struct sockaddr_in *)addr, out, cap);
    else if (addr->sa_family == AF_INET6)
        uv_ip6_name((const struct sockaddr_in6 *)addr, out, cap);
    return strlen(out);
}

/* The port actually bound, which is what a bind to port 0 is for. */
int32_t auv_tcp_port(uv_handle_t *h) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getsockname((const uv_tcp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    return auv__port_of(&ss);
}

/* The local and peer ends as text, either family, and their ports. A caller sizes its buffer
 * with a first call passing cap == 0, like every other string getter here. -1 for a port that
 * cannot be read: a unix-domain socket has none, and neither has an unconnected one. */
size_t auv_tcp_addr(uv_handle_t *h, char *buf, size_t cap) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getsockname((const uv_tcp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return 0;
    char text[INET6_ADDRSTRLEN + 1];
    size_t n = auv__addr_text((const struct sockaddr *)&ss, text, sizeof(text));
    return auv__copy_cstr_n(text, n, buf, cap);
}

size_t auv_tcp_peer_addr(uv_handle_t *h, char *buf, size_t cap) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getpeername((const uv_tcp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return 0;
    char text[INET6_ADDRSTRLEN + 1];
    size_t n = auv__addr_text((const struct sockaddr *)&ss, text, sizeof(text));
    return auv__copy_cstr_n(text, n, buf, cap);
}

int32_t auv_tcp_peer_port(uv_handle_t *h) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getpeername((const uv_tcp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    return auv__port_of(&ss);
}

/* Nagle off: send a small write immediately instead of waiting to coalesce it with the next
 * one. What an interactive protocol wants and a bulk transfer does not. */
int32_t auv_tcp_nodelay(uv_handle_t *h, int32_t on) {
    return uv_tcp_nodelay((uv_tcp_t *)h, on ? 1 : 0);
}

/* Keepalive probes after `delay` seconds of silence -- the only way to find out that a peer
 * has gone away without saying so, short of writing to it. */
int32_t auv_tcp_keepalive(uv_handle_t *h, int32_t on, int32_t delay_s) {
    return uv_tcp_keepalive((uv_tcp_t *)h, on ? 1 : 0, (unsigned int)(delay_s < 0 ? 0 : delay_s));
}

static void auv__on_connection(uv_stream_t *s, int status) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)s));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)s), AUV_K_CONNECTION, status, 0, NULL);
}

int32_t auv_listen(uv_handle_t *h, int32_t backlog) {
    return uv_listen((uv_stream_t *)h, backlog, auv__on_connection);
}

int32_t auv_accept(uv_handle_t *server, uv_handle_t *client) {
    return uv_accept((uv_stream_t *)server, (uv_stream_t *)client);
}

static void auv__on_connect(uv_connect_t *req, int status) {
    auv_creq *c = (auv_creq *)req;
    auv_slot *slot = c->slot;
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)req->handle));
    free(c);
    auv_slot_deliver(slot, AUV_K_CONNECT, status, 0, NULL);
}

/* Request-based operations take the waiter's slot explicitly rather than reading it off the
 * handle. A stream's own slot belongs to its READS, and reading continues across a write --
 * so a write completion delivered into it would wake a reader with the wrong answer. */
int32_t auv_tcp_connect(uv_handle_t *h, const char *ip, int32_t port, auv_slot *slot) {
    struct sockaddr_storage ss;
    int r = auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    auv_creq *c = calloc(1, sizeof(auv_creq));
    if (!c)
        return UV_ENOMEM;
    c->slot = slot;
    r = uv_tcp_connect(&c->req, (uv_tcp_t *)h, (const struct sockaddr *)&ss, auv__on_connect);
    if (r != 0)
        free(c);
    return r;
}

/* --- unix-domain sockets ------------------------------------------------------------------ */

/* auv_pipe_new + auv_pipe_open adopt a descriptor somebody else opened. These two are the
 * other half: a socket reached BY PATH, which is how anything gets to an LSP server, a docker
 * socket or a systemd-provided one. Everything above them is the ordinary stream API -- a
 * uv_pipe_t IS a uv_stream_t, so listen, accept, read, write and shutdown are already written.
 *
 * The filesystem entry is libuv's to remove: uv__pipe_close unlinks whatever uv_pipe_bind
 * created, so closing a listener leaves no stale socket behind. */
int32_t auv_pipe_bind(uv_handle_t *h, const char *path) {
    return uv_pipe_bind((uv_pipe_t *)h, path);
}

/* uv_pipe_connect returns void: EVERY outcome, a path that does not exist included, arrives at
 * the callback rather than here. So this cannot fail synchronously and its 0 says only "the
 * request is in flight" -- the answer is the completion, exactly as for a TCP connect that got
 * as far as being queued. (uv_pipe_connect2 does return a code, but it is libuv >= 1.46 and
 * this way needs no version test.) */
int32_t auv_pipe_connect(uv_handle_t *h, const char *path, auv_slot *slot) {
    auv_creq *c = calloc(1, sizeof(auv_creq));
    if (!c)
        return UV_ENOMEM;
    c->slot = slot;
    uv_pipe_connect(&c->req, (uv_pipe_t *)h, path, auv__on_connect);
    return 0;
}

/* The path a pipe is bound or connected to, and the one at the other end. Sized by a first call
 * with cap == 0, like every other string getter here.
 *
 * The type check is not belt and braces. uv_pipe_getsockname does not verify what it was handed
 * and reads whatever getsockname wrote as a sockaddr_un, so asking a TCP socket for its path
 * answers with the bytes of a sockaddr_in rather than with an error -- which a test caught,
 * since "" is what a caller has every reason to expect there. */
static int auv__is_pipe(const uv_handle_t *h) {
    return h && uv_handle_get_type(h) == UV_NAMED_PIPE;
}

size_t auv_pipe_path(uv_handle_t *h, char *buf, size_t cap) {
    if (!auv__is_pipe(h))
        return 0;
    char stack[4096];
    size_t n = sizeof(stack);
    if (uv_pipe_getsockname((const uv_pipe_t *)h, stack, &n) != 0)
        return 0;
    return auv__copy_cstr_n(stack, n, buf, cap);
}

size_t auv_pipe_peer_path(uv_handle_t *h, char *buf, size_t cap) {
    if (!auv__is_pipe(h))
        return 0;
    char stack[4096];
    size_t n = sizeof(stack);
    if (uv_pipe_getpeername((const uv_pipe_t *)h, stack, &n) != 0)
        return 0;
    return auv__copy_cstr_n(stack, n, buf, cap);
}

/* Who may connect. A socket in the filesystem gets the creating process's umask by default,
 * which for a service that other users are meant to reach is usually not what was wanted.
 * `flags` is auv_pipe_readable/auv_pipe_writable, added together. */
int32_t auv_pipe_chmod(uv_handle_t *h, int32_t flags) {
    if (!auv__is_pipe(h))
        return UV_EINVAL;
    return uv_pipe_chmod((uv_pipe_t *)h, flags);
}

int32_t auv_pipe_readable_flag(void) { return UV_READABLE; }
int32_t auv_pipe_writable_flag(void) { return UV_WRITABLE; }

/* WHO is at the other end of a unix-domain socket, as the kernel knows it rather than as the
 * peer claims: the one authentication a local socket can do without a protocol of its own.
 *
 * libuv does not expose this, so it goes straight to the socket option, and that option is not
 * standardized: Linux spells it SO_PEERCRED with a struct ucred, the BSDs have getpeereid() or
 * LOCAL_PEERCRED. Only the first is implemented here; anywhere else this reports ENOSYS rather
 * than guessing, which a caller can act on. `out` is three int32s: pid, uid, gid. */
int32_t auv_pipe_peer_cred(uv_handle_t *h, int32_t *out) {
#if defined(SO_PEERCRED) && defined(__linux__)
    uv_os_fd_t fd;
    if (uv_fileno(h, &fd) != 0)
        return UV_EBADF;
    /* `struct ucred` by name needs _GNU_SOURCE, which is a large thing to turn on for one
     * function -- and the shape is not glibc's to change anyway: it is the kernel's ABI for
     * SO_PEERCRED, three ids in this order. Spelled out here rather than reaching for the
     * feature-test macro. */
    struct { pid_t pid; uid_t uid; gid_t gid; } cred;
    socklen_t len = sizeof(cred);
    if (getsockopt((int)fd, SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0)
        return -errno;
    out[0] = (int32_t)cred.pid;
    out[1] = (int32_t)cred.uid;
    out[2] = (int32_t)cred.gid;
    return 0;
#else
    (void)h;
    (void)out;
    return UV_ENOSYS;
#endif
}

/* One chunk per read buffer, allocated here and owned by the slot once it lands. */
static void auv__on_alloc(uv_handle_t *h, size_t suggested, uv_buf_t *buf) {
    (void)h;
    size_t n = suggested > 65536 ? 65536 : suggested;
    buf->base = malloc(n);
    buf->len = buf->base ? n : 0;
}

static void auv__read_stop(uv_handle_t *h) {
    if (uv_read_stop((uv_stream_t *)h) == 0) {
        auv_loop_ctx *ctx = auv__ctx(uv_handle_get_loop(h));
        if (ctx)
            ctx->read_stops++;
    }
}

static void auv__on_read(uv_stream_t *s, ssize_t nread, const uv_buf_t *buf) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)s));
    auv_slot *slot = auv_handle_slot((uv_handle_t *)s);
    if (nread > 0) {
        auv_slot_deliver(slot, AUV_K_READ, 0, (int64_t)nread, buf->base);
    } else if (nread == 0) {
        free(buf->base);        /* nothing was ready after all */
        return;
    } else {
        free(buf->base);
        auv_slot_deliver(slot, nread == UV_EOF ? AUV_K_EOF : AUV_K_READ, (int64_t)nread, 0, NULL);
    }
    /* The delivery above resumed the reader, which has since either parked for the next
     * chunk or gone off to do something else. If it did not come back, stop reading: a
     * second chunk arriving now would have nowhere to go but the slot, and a third would be
     * dropped. Stopping is what turns a slow reader into backpressure on the sender. */
    if (!slot || auv_slot_state(slot) != AUV_PARKED)
        auv__read_stop((uv_handle_t *)s);
}

/* Idempotent: re-arming a stream that is already reading is the common case, because the
 * reader parks again from inside the callback that delivered the previous chunk. */
int32_t auv_read_start(uv_handle_t *h) {
    if (uv_is_active(h))
        return 0;
    return uv_read_start((uv_stream_t *)h, auv__on_alloc, auv__on_read);
}

int32_t auv_read_stop(uv_handle_t *h) {
    auv__read_stop(h);
    return 0;
}

static void auv__on_write_done(uv_write_t *req, int status) {
    auv_wreq *w = (auv_wreq *)req;
    auv_slot *slot = w->slot;
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)req->handle));
    free(w->buf.base);
    free(w);
    auv_slot_deliver(slot, AUV_K_WRITE, status, 0, NULL);
}

/* The bytes are copied: the caller's buffer is an Ante value whose lifetime nobody here can
 * reason about, and libuv holds on to what it is given until the write completes. */
int32_t auv_stream_write(uv_handle_t *h, const void *bytes, size_t n, auv_slot *slot) {
    auv_wreq *w = calloc(1, sizeof(auv_wreq));
    if (!w)
        return UV_ENOMEM;
    w->buf.base = malloc(n ? n : 1);
    if (!w->buf.base) {
        free(w);
        return UV_ENOMEM;
    }
    if (n)
        memcpy(w->buf.base, bytes, n);
    w->buf.len = n;
    w->slot = slot;
    /* uv_write copies the descriptor array, so a local one is enough; the BYTES have to
     * stay alive, which is what the request wrapper is for. */
    uv_buf_t bufs[1];
    bufs[0] = w->buf;
    int r = uv_write(&w->req, (uv_stream_t *)h, bufs, 1, auv__on_write_done);
    if (r != 0) {
        free(w->buf.base);
        free(w);
    }
    return r;
}

static void auv__on_shutdown(uv_shutdown_t *req, int status) {
    auv_sreq *s = (auv_sreq *)req;
    auv_slot *slot = s->slot;
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)req->handle));
    free(s);
    auv_slot_deliver(slot, AUV_K_SHUTDOWN, status, 0, NULL);
}

int32_t auv_stream_shutdown(uv_handle_t *h, auv_slot *slot) {
    auv_sreq *s = calloc(1, sizeof(auv_sreq));
    if (!s)
        return UV_ENOMEM;
    s->slot = slot;
    int r = uv_shutdown(&s->req, (uv_stream_t *)h, auv__on_shutdown);
    if (r != 0)
        free(s);
    return r;
}

int32_t auv_stream_readable(uv_handle_t *h) { return uv_is_readable((const uv_stream_t *)h); }
int32_t auv_stream_writable(uv_handle_t *h) { return uv_is_writable((const uv_stream_t *)h); }
int64_t auv_stream_write_queue(uv_handle_t *h) {
    return (int64_t)uv_stream_get_write_queue_size((const uv_stream_t *)h);
}

int32_t auv_stream_fileno(uv_handle_t *h) {
    uv_os_fd_t fd;
    if (uv_fileno(h, &fd) != 0)
        return -1;
    return (int32_t)fd;
}

/* --- udp ------------------------------------------------------------------------------------ */

/* A datagram socket is neither a handle you read nor one you accept from: every arrival carries
 * its own sender, and there is no connection to hang that on. So a received datagram is
 * marshalled as ONE payload with a fixed header:
 *
 *     int32 ip_len | int32 port | <ip_len bytes of sender address> | <the datagram>
 *
 * and the datagram's own length arrives in the completion's `aux`, exactly as a stream read's
 * does. Nothing is NUL-terminated and nothing needs Ante and C to agree on a struct.
 *
 * The handle is created with plain uv_udp_init and never UV_UDP_RECVMMSG, which is a decision
 * rather than an oversight: recvmmsg makes one callback per datagram out of ONE shared buffer
 * that the callback must not free until libuv says so, and the whole wait-slot pattern here is
 * built on a payload the slot owns outright. One syscall per datagram is the price. */

#define AUV_DGRAM_HEADER 8

typedef struct auv_ureq {
    uv_udp_send_t req;
    uv_buf_t      buf;
    auv_slot     *slot;
} auv_ureq;

static void *auv__marshal_dgram(const struct sockaddr *addr, const char *data, size_t n,
                                size_t *len_out) {
    char ip[INET6_ADDRSTRLEN + 1];
    size_t iplen = auv__addr_text(addr, ip, sizeof(ip));
    char *out = malloc(AUV_DGRAM_HEADER + iplen + (n ? n : 1));
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    int32_t hdr[2];
    hdr[0] = (int32_t)iplen;
    hdr[1] = addr ? auv__port_of((const struct sockaddr_storage *)addr) : -1;
    memcpy(out, hdr, AUV_DGRAM_HEADER);
    memcpy(out + AUV_DGRAM_HEADER, ip, iplen);
    if (n)
        memcpy(out + AUV_DGRAM_HEADER + iplen, data, n);
    *len_out = AUV_DGRAM_HEADER + iplen + n;
    return out;
}

uv_handle_t *auv_udp_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_UDP);
    if (!h)
        return NULL;
    if (uv_udp_init(loop, (uv_udp_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_udp_bind(uv_handle_t *h, const char *ip, int32_t port) {
    struct sockaddr_storage ss;
    int r = auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    return uv_udp_bind((uv_udp_t *)h, (const struct sockaddr *)&ss, 0);
}

int32_t auv_udp_port(uv_handle_t *h) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_udp_getsockname((const uv_udp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    return auv__port_of(&ss);
}

static void auv__on_udp_alloc(uv_handle_t *h, size_t suggested, uv_buf_t *buf) {
    (void)h;
    size_t n = suggested > 65536 ? 65536 : suggested;
    buf->base = malloc(n);
    buf->len = buf->base ? n : 0;
}

static void auv__on_udp_recv(uv_udp_t *h, ssize_t nread, const uv_buf_t *buf,
                             const struct sockaddr *addr, unsigned flags) {
    (void)flags;
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot *slot = auv_handle_slot((uv_handle_t *)h);

    /* nread == 0 WITH a sender is a real empty datagram; without one it means "nothing more was
     * ready", which is not an event at all. */
    if (nread == 0 && addr == NULL) {
        free(buf->base);
        return;
    }
    if (nread < 0) {
        free(buf->base);
        auv_slot_deliver(slot, AUV_K_DGRAM, (int64_t)nread, 0, NULL);
    } else {
        size_t n = 0;
        void *payload = auv__marshal_dgram(addr, buf->base, (size_t)nread, &n);
        free(buf->base);
        auv_slot_deliver(slot, AUV_K_DGRAM, 0, (int64_t)nread, payload);
    }
    /* Backpressure, exactly as for a stream: if the reader did not come straight back, stop
     * receiving. A datagram this socket has not read stays in the kernel's buffer, where it can
     * still be dropped -- but by the kernel, under a rule the program can see, rather than
     * silently here. */
    if (!slot || auv_slot_state(slot) != AUV_PARKED)
        uv_udp_recv_stop(h);
}

/* Idempotent, like auv_read_start and for the same reason: the reader re-arms from inside the
 * callback that delivered the previous datagram.
 *
 * NOT with uv_is_active, which is auv_read_start's test and would be wrong here. uv_udp_send
 * calls uv__handle_start to keep the loop alive until the datagram is on the wire, so a socket
 * that has only ever SENT reports itself active -- and skipping the recv_start on that basis is
 * a socket that never receives anything. (It cost a test to find: the asker in UdpTests sends
 * before it receives, which is the ordinary shape of a request.) uv_udp_recv_start's own
 * UV_EALREADY is the exact question, so it is what gets asked. */
int32_t auv_udp_recv_start(uv_handle_t *h) {
    int r = uv_udp_recv_start((uv_udp_t *)h, auv__on_udp_alloc, auv__on_udp_recv);
    return r == UV_EALREADY ? 0 : r;
}

int32_t auv_udp_recv_stop(uv_handle_t *h) { return uv_udp_recv_stop((uv_udp_t *)h); }

static void auv__on_udp_send(uv_udp_send_t *req, int status) {
    auv_ureq *w = (auv_ureq *)req;
    auv_slot *slot = w->slot;
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)req->handle));
    free(w->buf.base);
    free(w);
    auv_slot_deliver(slot, AUV_K_WRITE, status, 0, NULL);
}

/* The bytes are copied for the reason auv_stream_write copies them: libuv borrows the memory
 * until the send completes and says nothing about it until then.
 *
 * A NULL or empty `ip` means "wherever this socket is connected", which is the only thing a
 * connected socket accepts -- passing an address to one is EISCONN, and not passing one to an
 * unconnected socket is EDESTADDRREQ. Both are libuv's answers, not this file's. */
int32_t auv_udp_send(uv_handle_t *h, const void *bytes, size_t n, const char *ip, int32_t port,
                     auv_slot *slot) {
    struct sockaddr_storage ss;
    int connected = (ip == NULL || ip[0] == '\0');
    int r = connected ? 0 : auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    auv_ureq *w = calloc(1, sizeof(auv_ureq));
    if (!w)
        return UV_ENOMEM;
    w->buf.base = malloc(n ? n : 1);
    if (!w->buf.base) {
        free(w);
        return UV_ENOMEM;
    }
    if (n)
        memcpy(w->buf.base, bytes, n);
    w->buf.len = n;
    w->slot = slot;
    uv_buf_t bufs[1];
    bufs[0] = w->buf;
    r = uv_udp_send(&w->req, (uv_udp_t *)h, bufs, 1,
                    connected ? NULL : (const struct sockaddr *)&ss, auv__on_udp_send);
    if (r != 0) {
        free(w->buf.base);
        free(w);
    }
    return r;
}

/* Send without a request and without waiting: it goes now or it does not go at all (UV_EAGAIN).
 * The one datagram operation that needs no wait slot, because there is nothing to wait for --
 * which is also what makes it callable from a place that cannot park. */
int64_t auv_udp_try_send(uv_handle_t *h, const void *bytes, size_t n, const char *ip,
                         int32_t port) {
    struct sockaddr_storage ss;
    int connected = (ip == NULL || ip[0] == '\0');
    int r = connected ? 0 : auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    uv_buf_t buf = uv_buf_init((char *)(uintptr_t)bytes, (unsigned int)n);
    return uv_udp_try_send((uv_udp_t *)h, &buf, 1,
                           connected ? NULL : (const struct sockaddr *)&ss);
}

/* Fix a peer, so that sends need no address and datagrams from anyone else are dropped by the
 * kernel. An empty address disconnects again. */
int32_t auv_udp_connect(uv_handle_t *h, const char *ip, int32_t port) {
    if (ip == NULL || ip[0] == '\0')
        return uv_udp_connect((uv_udp_t *)h, NULL);
    struct sockaddr_storage ss;
    int r = auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    return uv_udp_connect((uv_udp_t *)h, (const struct sockaddr *)&ss);
}

size_t auv_udp_peer_addr(uv_handle_t *h, char *buf, size_t cap) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_udp_getpeername((const uv_udp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return 0;
    char text[INET6_ADDRSTRLEN + 1];
    size_t n = auv__addr_text((const struct sockaddr *)&ss, text, sizeof(text));
    return auv__copy_cstr_n(text, n, buf, cap);
}

int32_t auv_udp_peer_port(uv_handle_t *h) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_udp_getpeername((const uv_udp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    return auv__port_of(&ss);
}

size_t auv_udp_addr(uv_handle_t *h, char *buf, size_t cap) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_udp_getsockname((const uv_udp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return 0;
    char text[INET6_ADDRSTRLEN + 1];
    size_t n = auv__addr_text((const struct sockaddr *)&ss, text, sizeof(text));
    return auv__copy_cstr_n(text, n, buf, cap);
}

/* The knobs. Broadcast has to be asked for before the kernel will send to a broadcast address;
 * TTL bounds how far a datagram travels; the multicast pair decides whether a group send comes
 * back to this host and how far it goes. */
int32_t auv_udp_set_broadcast(uv_handle_t *h, int32_t on) {
    return uv_udp_set_broadcast((uv_udp_t *)h, on ? 1 : 0);
}

int32_t auv_udp_set_ttl(uv_handle_t *h, int32_t ttl) {
    return uv_udp_set_ttl((uv_udp_t *)h, ttl);
}

int32_t auv_udp_set_multicast_loop(uv_handle_t *h, int32_t on) {
    return uv_udp_set_multicast_loop((uv_udp_t *)h, on ? 1 : 0);
}

int32_t auv_udp_set_multicast_ttl(uv_handle_t *h, int32_t ttl) {
    return uv_udp_set_multicast_ttl((uv_udp_t *)h, ttl);
}

/* Join or leave a multicast group. `iface` may be empty for "whichever interface the routing
 * table picks", which is what a single-homed host wants. */
int32_t auv_udp_membership(uv_handle_t *h, const char *group, const char *iface, int32_t join) {
    return uv_udp_set_membership((uv_udp_t *)h, group,
                                 (iface && iface[0]) ? iface : NULL,
                                 join ? UV_JOIN_GROUP : UV_LEAVE_GROUP);
}

/* --- filesystem ------------------------------------------------------------------------- */

/* A filesystem request runs on libuv's threadpool but completes on the LOOP thread, which is
 * what makes it safe to resume a coroutine from here at all. Every trampoline below runs the
 * thread check, so the guarantee is tested rather than assumed. */

#define AUV_FS_OPEN     1
#define AUV_FS_READ     2
#define AUV_FS_WRITE    3
#define AUV_FS_CLOSE    4
#define AUV_FS_STAT     5
#define AUV_FS_SCANDIR  6
#define AUV_FS_MKDIR    7
#define AUV_FS_UNLINK   8
#define AUV_FS_RENAME   9
#define AUV_FS_RMDIR   10
/* The second batch. Each is one case in each of the two switches below and one wrapper in
 * Fs.an, which is the cheapest breadth per line anywhere in this binding. */
#define AUV_FS_FSYNC       11
#define AUV_FS_FDATASYNC   12
#define AUV_FS_FTRUNCATE   13
#define AUV_FS_LSTAT       14
#define AUV_FS_FSTAT       15
#define AUV_FS_ACCESS      16
#define AUV_FS_CHMOD       17
#define AUV_FS_CHOWN       18
#define AUV_FS_UTIME       19
#define AUV_FS_LINK        20
#define AUV_FS_SYMLINK     21
#define AUV_FS_READLINK    22
#define AUV_FS_REALPATH    23
#define AUV_FS_COPYFILE    24
#define AUV_FS_MKDTEMP     25
#define AUV_FS_STATFS      26

typedef struct auv_freq {
    uv_fs_t   req;
    auv_slot *slot;
    int32_t   op;
    char     *buf;    /* the read buffer, or a copy of the bytes being written */
    size_t    len;
} auv_freq;

/* Flag values differ across platforms, so they are read rather than hardcoded. */
int32_t auv_o_rdonly(void) { return O_RDONLY; }
int32_t auv_o_wronly(void) { return O_WRONLY; }
int32_t auv_o_rdwr(void)   { return O_RDWR; }
int32_t auv_o_creat(void)  { return O_CREAT; }
int32_t auv_o_trunc(void)  { return O_TRUNC; }
int32_t auv_o_append(void) { return O_APPEND; }
int32_t auv_o_excl(void)   { return O_EXCL; }

/* access(2) modes and copyfile flags, read for the same reason. */
int32_t auv_f_ok(void) { return F_OK; }
int32_t auv_r_ok(void) { return R_OK; }
int32_t auv_w_ok(void) { return W_OK; }
int32_t auv_x_ok(void) { return X_OK; }
int32_t auv_copyfile_excl(void)    { return UV_FS_COPYFILE_EXCL; }
int32_t auv_copyfile_ficlone(void) { return UV_FS_COPYFILE_FICLONE; }

/* A stat result as four int64s: size, mode, directory flag, modification time in seconds.
 * A packed record of scalars rather than a struct layout Ante would have to agree with. */
#define AUV_STAT_FIELDS 4

static void *auv__marshal_stat(const uv_stat_t *st, size_t *len_out) {
    int64_t *out = malloc(AUV_STAT_FIELDS * sizeof(int64_t));
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    out[0] = (int64_t)st->st_size;
    out[1] = (int64_t)st->st_mode;
    out[2] = S_ISDIR(st->st_mode) ? 1 : 0;
    out[3] = (int64_t)st->st_mtim.tv_sec;
    *len_out = AUV_STAT_FIELDS * sizeof(int64_t);
    return out;
}

/* A filesystem's own numbers, packed the way a stat is. `f_type` is the one field the
 * synchronous path cannot fill -- see auv_fs_sync -- so a caller comparing the two handlers
 * should not read it. */
#define AUV_STATFS_FIELDS 7

static void *auv__marshal_statfs(const uv_statfs_t *st, size_t *len_out) {
    int64_t *out = malloc(AUV_STATFS_FIELDS * sizeof(int64_t));
    if (!out || !st) {
        free(out);
        *len_out = 0;
        return NULL;
    }
    out[0] = (int64_t)st->f_type;
    out[1] = (int64_t)st->f_bsize;
    out[2] = (int64_t)st->f_blocks;
    out[3] = (int64_t)st->f_bfree;
    out[4] = (int64_t)st->f_bavail;
    out[5] = (int64_t)st->f_files;
    out[6] = (int64_t)st->f_ffree;
    *len_out = AUV_STATFS_FIELDS * sizeof(int64_t);
    return out;
}

/* A NUL-terminated result -- a link target, a resolved path, the directory mkdtemp made -- as
 * raw bytes and a length, which is how every payload here travels. The Ante side copies `aux`
 * bytes and never looks for a terminator. */
static void *auv__marshal_cstr(const char *s, size_t *len_out) {
    if (!s) {
        *len_out = 0;
        return NULL;
    }
    size_t n = strlen(s);
    char *out = malloc(n ? n : 1);
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    memcpy(out, s, n);
    *len_out = n;
    return out;
}

/* Directory entries as newline-separated names. uv_fs_req_cleanup releases libuv's own copy,
 * so the names are marshalled before the request is cleaned up. */
/* A scandir entry is marshalled as one line, "<T><name>\n", where T is a single byte saying what
 * the entry IS: 'd' directory, 'f' anything else, '?' don't know. The type is free here -- readdir
 * already returned it -- and it is the difference between a caller listing a directory in one
 * round trip and a caller following it with a stat per entry, which is what pc's file-name
 * completion used to do. '?' is not hypothetical: d_type is DT_UNKNOWN on filesystems that do not
 * carry it (XFS without ftype, some network mounts), and a caller that needs certainty has to stat
 * those few itself.
 *
 * The type byte is always exactly one byte, so a name is the rest of its line and nothing about
 * this is ambiguous for a name that itself begins with 'd' or '?'. */
static char auv__dirent_type_char(uv_dirent_type_t t) {
    if (t == UV_DIRENT_DIR)
        return 'd';
    if (t == UV_DIRENT_UNKNOWN)
        return '?';
    return 'f';
}

static void *auv__marshal_scandir(uv_fs_t *req, size_t *len_out) {
    size_t cap = 256, len = 0;
    char *out = malloc(cap);
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    uv_dirent_t ent;
    while (uv_fs_scandir_next(req, &ent) != UV_EOF) {
        size_t n = strlen(ent.name);
        if (len + n + 2 > cap) {
            size_t want = cap;
            while (want < len + n + 2)
                want *= 2;
            char *p = realloc(out, want);
            if (!p)
                break;
            out = p;
            cap = want;
        }
        out[len++] = auv__dirent_type_char(ent.type);
        memcpy(out + len, ent.name, n);
        len += n;
        out[len++] = '\n';
    }
    *len_out = len;
    return out;
}

static void auv__on_fs(uv_fs_t *req) {
    auv_freq *f = (auv_freq *)req;
    auv__check_thread(req->loop);
    auv_slot *slot = f->slot;
    int64_t status = (int64_t)req->result;
    int64_t aux = 0;
    void *payload = NULL;

    if (status >= 0) {
        size_t n = 0;
        switch (f->op) {
        case AUV_FS_READ:
            payload = f->buf;      /* the slot owns the buffer from here */
            f->buf = NULL;
            aux = status;
            break;
        /* All three fill req->statbuf; which of the three asked is the only difference. */
        case AUV_FS_STAT:
        case AUV_FS_LSTAT:
        case AUV_FS_FSTAT:
            payload = auv__marshal_stat(&req->statbuf, &n);
            aux = (int64_t)n;
            break;
        case AUV_FS_SCANDIR:
            payload = auv__marshal_scandir(req, &n);
            aux = (int64_t)n;
            break;
        /* libuv leaves a string result in req->ptr, and the one directory mkdtemp made in
         * req->path. Both belong to the request, so both are copied before the cleanup. */
        case AUV_FS_READLINK:
        case AUV_FS_REALPATH:
            payload = auv__marshal_cstr((const char *)req->ptr, &n);
            aux = (int64_t)n;
            break;
        case AUV_FS_MKDTEMP:
            payload = auv__marshal_cstr(req->path, &n);
            aux = (int64_t)n;
            break;
        case AUV_FS_STATFS:
            payload = auv__marshal_statfs((const uv_statfs_t *)req->ptr, &n);
            aux = (int64_t)n;
            break;
        default:
            break;
        }
    }

    uv_fs_req_cleanup(req);
    free(f->buf);
    free(f);
    auv_slot_deliver(slot, AUV_K_FS, status, aux, payload);
}

/* One entry point for the whole filesystem surface: `op` picks the call, `a`/`b`/`c` are its
 * scalars and `p`/`q` its paths or bytes. Keeping it to one FFI signature is what keeps the
 * prototypes the compiler synthesizes from drifting apart. */
int32_t auv_fs_submit(uv_loop_t *loop, int32_t op, int64_t a, int64_t b, int64_t c,
                      const char *p, const char *q, auv_slot *slot) {
    auv_freq *f = calloc(1, sizeof(auv_freq));
    if (!f)
        return UV_ENOMEM;
    f->slot = slot;
    f->op = op;

    int r = UV_EINVAL;
    switch (op) {
    case AUV_FS_OPEN:
        r = uv_fs_open(loop, &f->req, p, (int)a, (int)b, auv__on_fs);
        break;
    case AUV_FS_READ: {
        size_t n = (size_t)b;
        f->buf = malloc(n ? n : 1);
        if (!f->buf) {
            free(f);
            return UV_ENOMEM;
        }
        f->len = n;
        uv_buf_t buf = uv_buf_init(f->buf, (unsigned int)n);
        r = uv_fs_read(loop, &f->req, (uv_file)a, &buf, 1, (int64_t)c, auv__on_fs);
        break;
    }
    case AUV_FS_WRITE: {
        size_t n = (size_t)b;
        f->buf = malloc(n ? n : 1);
        if (!f->buf) {
            free(f);
            return UV_ENOMEM;
        }
        if (n)
            memcpy(f->buf, p, n);
        f->len = n;
        uv_buf_t buf = uv_buf_init(f->buf, (unsigned int)n);
        r = uv_fs_write(loop, &f->req, (uv_file)a, &buf, 1, (int64_t)c, auv__on_fs);
        break;
    }
    case AUV_FS_CLOSE:   r = uv_fs_close(loop, &f->req, (uv_file)a, auv__on_fs); break;
    case AUV_FS_STAT:    r = uv_fs_stat(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_SCANDIR: r = uv_fs_scandir(loop, &f->req, p, 0, auv__on_fs); break;
    case AUV_FS_MKDIR:   r = uv_fs_mkdir(loop, &f->req, p, (int)a, auv__on_fs); break;
    case AUV_FS_UNLINK:  r = uv_fs_unlink(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_RENAME:  r = uv_fs_rename(loop, &f->req, p, q, auv__on_fs); break;
    case AUV_FS_RMDIR:   r = uv_fs_rmdir(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_FSYNC:     r = uv_fs_fsync(loop, &f->req, (uv_file)a, auv__on_fs); break;
    case AUV_FS_FDATASYNC: r = uv_fs_fdatasync(loop, &f->req, (uv_file)a, auv__on_fs); break;
    case AUV_FS_FTRUNCATE: r = uv_fs_ftruncate(loop, &f->req, (uv_file)a, b, auv__on_fs); break;
    case AUV_FS_LSTAT:     r = uv_fs_lstat(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_FSTAT:     r = uv_fs_fstat(loop, &f->req, (uv_file)a, auv__on_fs); break;
    case AUV_FS_ACCESS:    r = uv_fs_access(loop, &f->req, p, (int)a, auv__on_fs); break;
    case AUV_FS_CHMOD:     r = uv_fs_chmod(loop, &f->req, p, (int)a, auv__on_fs); break;
    case AUV_FS_CHOWN:     r = uv_fs_chown(loop, &f->req, p, (uv_uid_t)a, (uv_gid_t)b, auv__on_fs); break;
    /* Seconds as doubles is uv_fs_utime's own signature; sub-second times do not survive the
     * int64 the submission carries, which is the one place this batch loses precision. */
    case AUV_FS_UTIME:     r = uv_fs_utime(loop, &f->req, p, (double)a, (double)b, auv__on_fs); break;
    case AUV_FS_LINK:      r = uv_fs_link(loop, &f->req, p, q, auv__on_fs); break;
    case AUV_FS_SYMLINK:   r = uv_fs_symlink(loop, &f->req, p, q, (int)a, auv__on_fs); break;
    case AUV_FS_READLINK:  r = uv_fs_readlink(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_REALPATH:  r = uv_fs_realpath(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_COPYFILE:  r = uv_fs_copyfile(loop, &f->req, p, q, (int)a, auv__on_fs); break;
    case AUV_FS_MKDTEMP:   r = uv_fs_mkdtemp(loop, &f->req, p, auv__on_fs); break;
    case AUV_FS_STATFS:    r = uv_fs_statfs(loop, &f->req, p, auv__on_fs); break;
    default: break;
    }

    if (r != 0) {
        free(f->buf);
        free(f);
    }
    return r;
}

/* The same four fields out of a libc `struct stat`, for the synchronous path. uv_stat_t and
 * struct stat are different types with the same information, so the packing is spelled twice
 * and the LAYOUT exactly once -- AUV_STAT_FIELDS and the order below. */
static void *auv__marshal_stat_raw(const struct stat *st, size_t *len_out) {
    int64_t *out = malloc(AUV_STAT_FIELDS * sizeof(int64_t));
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    out[0] = (int64_t)st->st_size;
    out[1] = (int64_t)st->st_mode;
    out[2] = S_ISDIR(st->st_mode) ? 1 : 0;
    out[3] = (int64_t)st->st_mtim.tv_sec;
    *len_out = AUV_STAT_FIELDS * sizeof(int64_t);
    return out;
}

/* uv_fs_copyfile is the one operation in the batch with no libc twin, so here is the loop it
 * stands for. UV_FS_COPYFILE_EXCL is honored; the two reflink flags are not, because a reflink
 * is a filesystem's optimization of this and copying the bytes is always a correct answer.
 * Returns 0 or -errno, like every other arm of auv_fs_sync. */
static int64_t auv__copyfile_sync(const char *src, const char *dst, int flags) {
    int in = open(src, O_RDONLY);
    if (in < 0)
        return -errno;

    struct stat st;
    mode_t mode = fstat(in, &st) == 0 ? (st.st_mode & 07777) : 0644;
    int oflags = O_WRONLY | O_CREAT | ((flags & UV_FS_COPYFILE_EXCL) ? O_EXCL : O_TRUNC);
    int out = open(dst, oflags, mode);
    if (out < 0) {
        int e = errno;
        close(in);
        return -e;
    }

    char buf[65536];
    int64_t rc = 0;
    for (;;) {
        ssize_t n = read(in, buf, sizeof(buf));
        if (n == 0)
            break;
        if (n < 0) {
            rc = -errno;
            break;
        }
        ssize_t off = 0;
        while (off < n) {
            ssize_t w = write(out, buf + off, (size_t)(n - off));
            if (w < 0) {
                rc = -errno;
                break;
            }
            off += w;
        }
        if (rc != 0)
            break;
    }

    /* A close that fails is a write that failed, so it is reported when nothing else has been. */
    if (close(out) < 0 && rc == 0)
        rc = -errno;
    close(in);
    return rc;
}

/* The synchronous twin, for programs run with no loop and no task at all. Plain libc, not
 * libuv with the callback left out: there is no loop to hand libuv in that setting. The
 * completion is stored in the slot exactly as the asynchronous path would leave it.
 *
 * "Exactly" includes the offset convention, which is easy to miss because only one of the two
 * paths spells it out: a NEGATIVE offset means "wherever the file position already is", which
 * is what uv_fs_read/uv_fs_write do with -1 and the only thing a FIFO, a pipe or a character
 * device can mean. pread/pwrite cannot express it -- they reject it with EINVAL -- so those
 * are for a real offset only and read/write serve the rest. Reading an unseekable file through
 * `Async.blocking` failed outright until this distinction was made. */
int32_t auv_fs_sync(int32_t op, int64_t a, int64_t b, int64_t c,
                    const char *p, const char *q, auv_slot *slot) {
    int64_t status = -EINVAL;
    int64_t aux = 0;
    void *payload = NULL;

    switch (op) {
    case AUV_FS_OPEN: {
        int fd = open(p, (int)a, (mode_t)b);
        status = fd < 0 ? -errno : fd;
        break;
    }
    case AUV_FS_READ: {
        size_t n = (size_t)b;
        char *buf = malloc(n ? n : 1);
        if (!buf)
            return UV_ENOMEM;
        ssize_t got = c < 0 ? read((int)a, buf, n) : pread((int)a, buf, n, (off_t)c);
        if (got < 0) {
            status = -errno;
            free(buf);
        } else {
            status = got;
            aux = got;
            payload = buf;
        }
        break;
    }
    case AUV_FS_WRITE: {
        ssize_t put = c < 0 ? write((int)a, p, (size_t)b)
                            : pwrite((int)a, p, (size_t)b, (off_t)c);
        status = put < 0 ? -errno : put;
        break;
    }
    case AUV_FS_CLOSE:
        status = close((int)a) < 0 ? -errno : 0;
        break;
    case AUV_FS_STAT: {
        struct stat st;
        if (stat(p, &st) < 0)
            status = -errno;
        else {
            size_t n = 0;
            payload = auv__marshal_stat_raw(&st, &n);
            if (!payload)
                return UV_ENOMEM;
            aux = (int64_t)n;
            status = 0;
        }
        break;
    }
    case AUV_FS_SCANDIR: {
        DIR *d = opendir(p);
        if (!d)
            status = -errno;
        else {
            size_t cap = 256, len = 0;
            char *out = malloc(cap);
            struct dirent *e;
            while (out && (e = readdir(d)) != NULL) {
                if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
                    continue;
                size_t n = strlen(e->d_name);
                if (len + n + 2 > cap) {
                    size_t want = cap;
                    while (want < len + n + 2)
                        want *= 2;
                    char *np = realloc(out, want);
                    if (!np)
                        break;
                    out = np;
                    cap = want;
                }
                /* The same one-byte type prefix the loop path emits, from readdir's own d_type. */
                out[len++] = e->d_type == DT_DIR ? 'd' : (e->d_type == DT_UNKNOWN ? '?' : 'f');
                memcpy(out + len, e->d_name, n);
                len += n;
                out[len++] = '\n';
            }
            closedir(d);
            payload = out;
            aux = (int64_t)len;
            status = 0;
        }
        break;
    }
    case AUV_FS_MKDIR:  status = mkdir(p, (mode_t)a) < 0 ? -errno : 0; break;
    case AUV_FS_UNLINK: status = unlink(p) < 0 ? -errno : 0; break;
    case AUV_FS_RENAME: status = rename(p, q) < 0 ? -errno : 0; break;
    case AUV_FS_RMDIR:  status = rmdir(p) < 0 ? -errno : 0; break;
    case AUV_FS_FSYNC:     status = fsync((int)a) < 0 ? -errno : 0; break;
    case AUV_FS_FDATASYNC: status = fdatasync((int)a) < 0 ? -errno : 0; break;
    case AUV_FS_FTRUNCATE: status = ftruncate((int)a, (off_t)b) < 0 ? -errno : 0; break;
    case AUV_FS_LSTAT:
    case AUV_FS_FSTAT: {
        struct stat st;
        int got = op == AUV_FS_FSTAT ? fstat((int)a, &st) : lstat(p, &st);
        if (got < 0)
            status = -errno;
        else {
            size_t n = 0;
            payload = auv__marshal_stat_raw(&st, &n);
            if (!payload)
                return UV_ENOMEM;
            aux = (int64_t)n;
            status = 0;
        }
        break;
    }
    case AUV_FS_ACCESS: status = access(p, (int)a) < 0 ? -errno : 0; break;
    case AUV_FS_CHMOD:  status = chmod(p, (mode_t)a) < 0 ? -errno : 0; break;
    case AUV_FS_CHOWN:  status = chown(p, (uid_t)a, (gid_t)b) < 0 ? -errno : 0; break;
    case AUV_FS_UTIME: {
        struct timeval tv[2];
        tv[0].tv_sec = (time_t)a;
        tv[0].tv_usec = 0;
        tv[1].tv_sec = (time_t)b;
        tv[1].tv_usec = 0;
        status = utimes(p, tv) < 0 ? -errno : 0;
        break;
    }
    case AUV_FS_LINK:    status = link(p, q) < 0 ? -errno : 0; break;
    case AUV_FS_SYMLINK: status = symlink(p, q) < 0 ? -errno : 0; break;
    case AUV_FS_READLINK: {
        char buf[PATH_MAX];
        ssize_t n = readlink(p, buf, sizeof(buf));
        if (n < 0)
            status = -errno;
        else {
            char *out = malloc(n ? (size_t)n : 1);
            if (!out)
                return UV_ENOMEM;
            memcpy(out, buf, (size_t)n);
            payload = out;
            aux = n;
            status = 0;
        }
        break;
    }
    case AUV_FS_REALPATH: {
        char *rp = realpath(p, NULL);
        if (!rp)
            status = -errno;
        else {
            size_t n = 0;
            payload = auv__marshal_cstr(rp, &n);
            free(rp);
            if (!payload)
                return UV_ENOMEM;
            aux = (int64_t)n;
            status = 0;
        }
        break;
    }
    case AUV_FS_COPYFILE: status = auv__copyfile_sync(p, q, (int)a); break;
    case AUV_FS_MKDTEMP: {
        /* mkdtemp rewrites its template in place, and `p` belongs to the caller. */
        char *tpl = auv_cstr_new(p, strlen(p));
        if (!tpl)
            return UV_ENOMEM;
        if (!mkdtemp(tpl))
            status = -errno;
        else {
            size_t n = 0;
            payload = auv__marshal_cstr(tpl, &n);
            aux = (int64_t)n;
            status = 0;
        }
        free(tpl);
        break;
    }
    case AUV_FS_STATFS: {
        /* statvfs(3) is POSIX and statfs(2) is not, so this is the portable one -- at the cost
         * of the filesystem TYPE, the single field statvfs does not carry. It is reported as 0
         * here and as the real thing on the loop path; a program that reads it should not
         * expect the two handlers to agree. */
        struct statvfs vfs;
        if (statvfs(p, &vfs) < 0)
            status = -errno;
        else {
            int64_t *out = malloc(AUV_STATFS_FIELDS * sizeof(int64_t));
            if (!out)
                return UV_ENOMEM;
            out[0] = 0;
            out[1] = (int64_t)vfs.f_bsize;
            out[2] = (int64_t)vfs.f_blocks;
            out[3] = (int64_t)vfs.f_bfree;
            out[4] = (int64_t)vfs.f_bavail;
            out[5] = (int64_t)vfs.f_files;
            out[6] = (int64_t)vfs.f_ffree;
            payload = out;
            aux = AUV_STATFS_FIELDS * sizeof(int64_t);
            status = 0;
        }
        break;
    }
    default: break;
    }

    auv_slot_deliver(slot, AUV_K_FS, status, aux, payload);
    return 0;
}

/* Read one field out of a marshalled stat payload. */
int64_t auv_stat_field(const void *payload, size_t i) {
    if (!payload || i >= AUV_STAT_FIELDS)
        return 0;
    return ((const int64_t *)payload)[i];
}

/* The same, for a statfs payload. */
int64_t auv_statfs_field(const void *payload, size_t i) {
    if (!payload || i >= AUV_STATFS_FIELDS)
        return 0;
    return ((const int64_t *)payload)[i];
}

/* --- file watching ---------------------------------------------------------------------------- */

/* Two handles for one job, because the two ways of asking are not interchangeable. uv_fs_event
 * is the kernel telling you (inotify, kqueue): immediate, cheap, and unavailable on some
 * filesystems -- network mounts especially. uv_fs_poll is a stat on a timer: it works
 * everywhere, costs a syscall per interval, and cannot see a change that is undone before the
 * next look. A program that must not miss anything on an NFS mount wants the second.
 *
 * An event is marshalled with a fixed header like a datagram's:
 *
 *     int32 renamed | int32 changed | <the name>
 *
 * and the name's length in `aux`. The two bits of uv's mask are split into two flags HERE
 * rather than handed over as a mask, because Ante has no bitwise operators: a mask it cannot
 * take apart is not an answer. A poll reports the file's CURRENT stat instead, packed exactly
 * as uv_fs_stat's is, so the Ante side reads it with the same accessor. */

#define AUV_FSEVENT_HEADER 8

int32_t auv_fs_event_recursive(void) { return UV_FS_EVENT_RECURSIVE; }

static void *auv__marshal_fsevent(const char *name, int events, size_t *len_out) {
    size_t n = name ? strlen(name) : 0;
    char *out = malloc(AUV_FSEVENT_HEADER + (n ? n : 1));
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    int32_t hdr[2];
    hdr[0] = (events & UV_RENAME) ? 1 : 0;
    hdr[1] = (events & UV_CHANGE) ? 1 : 0;
    memcpy(out, hdr, AUV_FSEVENT_HEADER);
    if (n)
        memcpy(out + AUV_FSEVENT_HEADER, name, n);
    *len_out = AUV_FSEVENT_HEADER + n;
    return out;
}

static void auv__on_fs_event(uv_fs_event_t *h, const char *filename, int events, int status) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    size_t n = 0;
    void *payload = status < 0 ? NULL : auv__marshal_fsevent(filename, events, &n);
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_FSEVENT, status,
                     (int64_t)(n > AUV_FSEVENT_HEADER ? n - AUV_FSEVENT_HEADER : 0), payload);
}

static void auv__on_fs_poll(uv_fs_poll_t *h, int status, const uv_stat_t *prev,
                            const uv_stat_t *curr) {
    (void)prev;
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    size_t n = 0;
    void *payload = (status == 0 && curr) ? auv__marshal_stat(curr, &n) : NULL;
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_FSEVENT, status, (int64_t)n,
                     payload);
}

uv_handle_t *auv_fs_event_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_FS_EVENT);
    if (!h)
        return NULL;
    if (uv_fs_event_init(loop, (uv_fs_event_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_fs_event_start(uv_handle_t *h, const char *path, int32_t flags) {
    return uv_fs_event_start((uv_fs_event_t *)h, auv__on_fs_event, path, (unsigned int)flags);
}

int32_t auv_fs_event_stop(uv_handle_t *h) { return uv_fs_event_stop((uv_fs_event_t *)h); }

uv_handle_t *auv_fs_poll_new(uv_loop_t *loop) {
    uv_handle_t *h = auv__alloc_handle(UV_FS_POLL);
    if (!h)
        return NULL;
    if (uv_fs_poll_init(loop, (uv_fs_poll_t *)h) != 0) {
        free(h);
        return NULL;
    }
    return h;
}

int32_t auv_fs_poll_start(uv_handle_t *h, const char *path, int32_t interval_ms) {
    return uv_fs_poll_start((uv_fs_poll_t *)h, auv__on_fs_poll, path,
                            (unsigned int)(interval_ms < 1 ? 1 : interval_ms));
}

int32_t auv_fs_poll_stop(uv_handle_t *h) { return uv_fs_poll_stop((uv_fs_poll_t *)h); }

/* --- name resolution --------------------------------------------------------------------- */

typedef struct auv_areq {
    uv_getaddrinfo_t req;
    auv_slot        *slot;
} auv_areq;

/* Resolved addresses as newline-separated numeric strings. */
static void *auv__marshal_addrs(struct addrinfo *ai, size_t *len_out) {
    size_t cap = 256, len = 0;
    char *out = malloc(cap);
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    for (struct addrinfo *p = ai; p != NULL; p = p->ai_next) {
        char text[INET6_ADDRSTRLEN + 1];
        text[0] = '\0';
        if (p->ai_family == AF_INET)
            uv_ip4_name((struct sockaddr_in *)p->ai_addr, text, sizeof(text));
        else if (p->ai_family == AF_INET6)
            uv_ip6_name((struct sockaddr_in6 *)p->ai_addr, text, sizeof(text));
        else
            continue;
        size_t n = strlen(text);
        if (n == 0)
            continue;
        if (len + n + 1 > cap) {
            size_t want = cap;
            while (want < len + n + 1)
                want *= 2;
            char *np = realloc(out, want);
            if (!np)
                break;
            out = np;
            cap = want;
        }
        memcpy(out + len, text, n);
        len += n;
        out[len++] = '\n';
    }
    *len_out = len;
    return out;
}

static void auv__on_addrinfo(uv_getaddrinfo_t *req, int status, struct addrinfo *res) {
    auv_areq *a = (auv_areq *)req;
    auv__check_thread(req->loop);
    auv_slot *slot = a->slot;
    size_t n = 0;
    void *payload = (status == 0) ? auv__marshal_addrs(res, &n) : NULL;
    if (res)
        uv_freeaddrinfo(res);
    free(a);
    auv_slot_deliver(slot, AUV_K_ADDRINFO, status, (int64_t)n, payload);
}

int32_t auv_getaddrinfo(uv_loop_t *loop, const char *node, const char *service, auv_slot *slot) {
    auv_areq *a = calloc(1, sizeof(auv_areq));
    if (!a)
        return UV_ENOMEM;
    a->slot = slot;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int r = uv_getaddrinfo(loop, &a->req, auv__on_addrinfo, node,
                           (service && service[0]) ? service : NULL, &hints);
    if (r != 0)
        free(a);
    return r;
}

/* --- reverse resolution ----------------------------------------------------------------- */

/* The other direction: an address to a name. Marshalled as two lines, host then service, which
 * is the same newline-separated shape the forward lookup uses. */

typedef struct auv_nreq {
    uv_getnameinfo_t req;
    auv_slot        *slot;
} auv_nreq;

static void *auv__marshal_nameinfo(const char *host, const char *service, size_t *len_out) {
    size_t hn = host ? strlen(host) : 0;
    size_t sn = service ? strlen(service) : 0;
    char *out = malloc(hn + sn + 2);
    if (!out) {
        *len_out = 0;
        return NULL;
    }
    if (hn)
        memcpy(out, host, hn);
    out[hn] = '\n';
    if (sn)
        memcpy(out + hn + 1, service, sn);
    out[hn + 1 + sn] = '\n';
    *len_out = hn + sn + 2;
    return out;
}

static void auv__on_nameinfo(uv_getnameinfo_t *req, int status, const char *hostname,
                             const char *service) {
    auv_nreq *a = (auv_nreq *)req;
    auv__check_thread(req->loop);
    auv_slot *slot = a->slot;
    size_t n = 0;
    void *payload = (status == 0) ? auv__marshal_nameinfo(hostname, service, &n) : NULL;
    free(a);
    auv_slot_deliver(slot, AUV_K_ADDRINFO, status, (int64_t)n, payload);
}

int32_t auv_getnameinfo(uv_loop_t *loop, const char *ip, int32_t port, auv_slot *slot) {
    struct sockaddr_storage ss;
    int r = auv__parse_addr(ip, port, &ss);
    if (r != 0)
        return r;
    auv_nreq *a = calloc(1, sizeof(auv_nreq));
    if (!a)
        return UV_ENOMEM;
    a->slot = slot;
    r = uv_getnameinfo(loop, &a->req, auv__on_nameinfo, (const struct sockaddr *)&ss, 0);
    if (r != 0)
        free(a);
    return r;
}

int32_t auv_getnameinfo_sync(const char *ip, int32_t port, auv_slot *slot) {
    struct sockaddr_storage ss;
    int r = auv__parse_addr(ip, port, &ss);
    if (r != 0) {
        auv_slot_deliver(slot, AUV_K_ADDRINFO, r, 0, NULL);
        return 0;
    }
    socklen_t len = (ss.ss_family == AF_INET6) ? sizeof(struct sockaddr_in6)
                                               : sizeof(struct sockaddr_in);
    char host[NI_MAXHOST];
    char serv[NI_MAXSERV];
    int rc = getnameinfo((const struct sockaddr *)&ss, len, host, sizeof(host), serv,
                         sizeof(serv), 0);
    size_t n = 0;
    void *payload = (rc == 0) ? auv__marshal_nameinfo(host, serv, &n) : NULL;
    auv_slot_deliver(slot, AUV_K_ADDRINFO, rc == 0 ? 0 : UV_EAI_NONAME, (int64_t)n, payload);
    return 0;
}

int32_t auv_getaddrinfo_sync(const char *node, const char *service, auv_slot *slot) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *res = NULL;
    int rc = getaddrinfo(node, (service && service[0]) ? service : NULL, &hints, &res);
    size_t n = 0;
    void *payload = (rc == 0) ? auv__marshal_addrs(res, &n) : NULL;
    if (res)
        freeaddrinfo(res);
    auv_slot_deliver(slot, AUV_K_ADDRINFO, rc == 0 ? 0 : UV_EAI_NONAME, (int64_t)n, payload);
    return 0;
}

/* --- child processes ---------------------------------------------------------------------- */

/* Spawning is where libuv's by-value aggregates are worst: uv_process_options_t holds an
 * array of uv_stdio_container_t, each a tagged union. All of it stays here; Ante hands over a
 * program name, a NULL-terminated argument vector and up to three pipe handles.
 *
 * The exit status arrives in the process handle's OWN slot, so a child that exits while its
 * output is still being read does not lose the notification. */

void *auv_ptr_at(void *const *p, size_t i) { return (void *)p[i]; }
void  auv_ptr_set(void **p, size_t i, void *v) { p[i] = v; }

static void auv__on_exit(uv_process_t *h, int64_t exit_status, int term_signal) {
    auv__check_thread(uv_handle_get_loop((uv_handle_t *)h));
    auv_slot_deliver(auv_handle_slot((uv_handle_t *)h), AUV_K_EXIT, exit_status, term_signal, NULL);
}

/* How each of the child's three standard streams is arranged. IGNORE hands it /dev/null;
 * PIPE gives it one end of a fresh pipe and this process the other; INHERIT hands over THIS
 * process's own descriptor, which is what a child that should share the terminal needs and the
 * one thing the first version of this could not express; FD hands over a specific descriptor. */
#define AUV_STDIO_IGNORE  0
#define AUV_STDIO_PIPE    1
#define AUV_STDIO_INHERIT 2
#define AUV_STDIO_FD      3

int32_t auv_stdio_ignore(void)  { return AUV_STDIO_IGNORE; }
int32_t auv_stdio_pipe(void)    { return AUV_STDIO_PIPE; }
int32_t auv_stdio_inherit(void) { return AUV_STDIO_INHERIT; }
int32_t auv_stdio_fd(void)      { return AUV_STDIO_FD; }

/* Everything uv_process_options_t can be told, as a heap record the Ante side fills in one
 * field at a time. A by-value aggregate with an array of tagged unions in it is exactly what
 * does not cross this boundary, so it is built here and never seen there. */
typedef struct auv_spawn_opts {
    char        **env;          /* NULL-terminated, or NULL to inherit this process's */
    int64_t       uid, gid;     /* < 0 for "leave alone" */
    int32_t       detached;
    int32_t       mode[3];      /* AUV_STDIO_* per stream */
    int32_t       fd[3];        /* for AUV_STDIO_FD */
    uv_handle_t  *pipe[3];      /* for AUV_STDIO_PIPE */
} auv_spawn_opts;

auv_spawn_opts *auv_spawn_opts_new(void) {
    auv_spawn_opts *o = calloc(1, sizeof(auv_spawn_opts));
    if (!o)
        return NULL;
    o->uid = -1;
    o->gid = -1;
    return o;
}

void auv_spawn_opts_free(auv_spawn_opts *o) { free(o); }

/* The vector is BORROWED for the spawn and belongs to the caller, exactly like the argument
 * vector next to it. */
void auv_spawn_opts_env(auv_spawn_opts *o, char **env) { if (o) o->env = env; }
void auv_spawn_opts_ids(auv_spawn_opts *o, int64_t uid, int64_t gid) {
    if (o) { o->uid = uid; o->gid = gid; }
}
void auv_spawn_opts_detached(auv_spawn_opts *o, int32_t on) { if (o) o->detached = on ? 1 : 0; }

void auv_spawn_opts_stdio(auv_spawn_opts *o, int32_t which, int32_t mode, int32_t fd,
                          uv_handle_t *pipe) {
    if (!o || which < 0 || which > 2)
        return;
    o->mode[which] = mode;
    o->fd[which] = fd;
    o->pipe[which] = pipe;
}

/* Returns the process handle, or NULL with the uv error in *err. `opts` may be NULL, which is
 * three ignored streams and nothing else set. */
uv_handle_t *auv_process_spawn2(uv_loop_t *loop, const char *file, char **args, const char *cwd,
                                const auv_spawn_opts *opts, int32_t *err) {
    uv_handle_t *h = auv__alloc_handle(UV_PROCESS);
    if (!h) {
        if (err)
            *err = UV_ENOMEM;
        return NULL;
    }

    uv_stdio_container_t stdio[3];
    memset(stdio, 0, sizeof(stdio));
    for (int i = 0; i < 3; i++) {
        int mode = opts ? opts->mode[i] : AUV_STDIO_IGNORE;
        switch (mode) {
        case AUV_STDIO_PIPE:
            /* Readable from the CHILD's side for stdin, writable for the other two. */
            stdio[i].flags = UV_CREATE_PIPE | (i == 0 ? UV_READABLE_PIPE : UV_WRITABLE_PIPE);
            stdio[i].data.stream = (uv_stream_t *)opts->pipe[i];
            break;
        case AUV_STDIO_INHERIT:
            stdio[i].flags = UV_INHERIT_FD;
            stdio[i].data.fd = i;
            break;
        case AUV_STDIO_FD:
            stdio[i].flags = UV_INHERIT_FD;
            stdio[i].data.fd = opts->fd[i];
            break;
        default:
            stdio[i].flags = UV_IGNORE;
            break;
        }
    }

    uv_process_options_t o;
    memset(&o, 0, sizeof(o));
    o.exit_cb = auv__on_exit;
    o.file = file;
    o.args = args;
    o.cwd = cwd;
    o.stdio_count = 3;
    o.stdio = stdio;
    if (opts) {
        o.env = opts->env;
        if (opts->detached)
            o.flags |= UV_PROCESS_DETACHED;
        if (opts->uid >= 0) {
            o.flags |= UV_PROCESS_SETUID;
            o.uid = (uv_uid_t)opts->uid;
        }
        if (opts->gid >= 0) {
            o.flags |= UV_PROCESS_SETGID;
            o.gid = (uv_gid_t)opts->gid;
        }
    }

    int r = uv_spawn(loop, (uv_process_t *)h, &o);
    if (r != 0) {
        /* uv_spawn initializes the handle even when it fails, so it has to be closed rather
         * than freed outright. */
        uv_close(h, auv__on_close);
        if (err)
            *err = r;
        return NULL;
    }
    if (err)
        *err = 0;
    return h;
}

/* The original three-pipe spawn, which is what most callers want: each pipe handle given, or
 * NULL for a stream the child should not get. */
uv_handle_t *auv_process_spawn(uv_loop_t *loop, const char *file, char **args, const char *cwd,
                               uv_handle_t *in_pipe, uv_handle_t *out_pipe, uv_handle_t *err_pipe,
                               int32_t *err) {
    auv_spawn_opts opts;
    memset(&opts, 0, sizeof(opts));
    opts.uid = -1;
    opts.gid = -1;
    uv_handle_t *pipes[3] = { in_pipe, out_pipe, err_pipe };
    for (int i = 0; i < 3; i++) {
        opts.mode[i] = pipes[i] ? AUV_STDIO_PIPE : AUV_STDIO_IGNORE;
        opts.pipe[i] = pipes[i];
    }
    return auv_process_spawn2(loop, file, args, cwd, &opts, err);
}

/* A detached child is released from this process's group so that it survives it; until then it
 * is still a handle the loop counts, and auv_unref up in the handle section is what says
 * otherwise. Nothing process-specific is needed for it. */

int32_t auv_process_kill(uv_handle_t *h, int32_t signum) {
    return uv_process_kill((uv_process_t *)h, signum);
}

int32_t auv_process_pid(uv_handle_t *h) { return (int32_t)uv_process_get_pid((uv_process_t *)h); }

/* --- what the machine is ------------------------------------------------------------------ */

/* None of this touches the loop: it is the platform answering about itself, which libuv already
 * knows how to ask portably. Every string getter takes the size-then-fill shape the rest of the
 * file uses -- a first call with cap == 0 reports the length.
 *
 * Errors come back as a NEGATIVE length is not expressible here, so a failed query reports 0
 * bytes and the caller gets an empty string. The ones where the difference matters (chdir,
 * setenv, priority) return a code instead. */

static size_t auv__os_string(int (*fn)(char *, size_t *), char *buf, size_t cap) {
    char stack[4096];
    size_t n = sizeof(stack);
    if (fn(stack, &n) != 0)
        return 0;
    return auv__copy_cstr_n(stack, n, buf, cap);
}

size_t auv_os_cwd(char *buf, size_t cap)     { return auv__os_string(uv_cwd, buf, cap); }
size_t auv_os_exepath(char *buf, size_t cap) { return auv__os_string(uv_exepath, buf, cap); }
size_t auv_os_homedir(char *buf, size_t cap) { return auv__os_string(uv_os_homedir, buf, cap); }
size_t auv_os_tmpdir(char *buf, size_t cap)  { return auv__os_string(uv_os_tmpdir, buf, cap); }

int32_t auv_os_chdir(const char *path) { return uv_chdir(path); }

size_t auv_os_hostname(char *buf, size_t cap) {
    char name[UV_MAXHOSTNAMESIZE];
    size_t n = sizeof(name);
    if (uv_os_gethostname(name, &n) != 0)
        return 0;
    return auv__copy_cstr_n(name, n, buf, cap);
}

/* uname's four parts, by index: 0 sysname, 1 release, 2 version, 3 machine. */
size_t auv_os_uname(int32_t which, char *buf, size_t cap) {
    uv_utsname_t u;
    if (uv_os_uname(&u) != 0)
        return 0;
    const char *s = which == 0 ? u.sysname
                  : which == 1 ? u.release
                  : which == 2 ? u.version
                  : which == 3 ? u.machine
                  : NULL;
    return auv__copy_cstr(s, buf, cap);
}

/* Seconds since boot. libuv reports a double; whole seconds is what anyone formats. */
int64_t auv_os_uptime(void) {
    double up = 0;
    if (uv_uptime(&up) != 0)
        return -1;
    return (int64_t)up;
}

int64_t auv_os_free_memory(void)  { return (int64_t)uv_get_free_memory(); }
int64_t auv_os_total_memory(void) { return (int64_t)uv_get_total_memory(); }

/* What a cgroup allows, which on a container is the number that matters and is not the same as
 * the machine's total. 0 when there is no limit. */
int64_t auv_os_constrained_memory(void) { return (int64_t)uv_get_constrained_memory(); }

/* How many threads it is worth running, which is not always how many CPUs exist: it honors the
 * container's allowance where there is one. */
int32_t auv_os_cpu_count(void) { return (int32_t)uv_available_parallelism(); }

/* uv_cpu_info allocates a fresh array per call, so model and speed take one query each rather
 * than holding the array across the FFI boundary. A caller reading every core pays for it;
 * nothing in this binding does that in a loop. */
size_t auv_os_cpu_model(int32_t i, char *buf, size_t cap) {
    uv_cpu_info_t *cpus = NULL;
    int count = 0;
    if (uv_cpu_info(&cpus, &count) != 0)
        return 0;
    size_t n = (i >= 0 && i < count) ? auv__copy_cstr(cpus[i].model, buf, cap) : 0;
    uv_free_cpu_info(cpus, count);
    return n;
}

int64_t auv_os_cpu_speed(int32_t i) {
    uv_cpu_info_t *cpus = NULL;
    int count = 0;
    if (uv_cpu_info(&cpus, &count) != 0)
        return 0;
    int64_t mhz = (i >= 0 && i < count) ? cpus[i].speed : 0;
    uv_free_cpu_info(cpus, count);
    return mhz;
}

/* The three load averages, by index. Zero everywhere Windows. */
int64_t auv_os_loadavg_milli(int32_t i) {
    double avg[3];
    uv_loadavg(avg);
    if (i < 0 || i > 2)
        return 0;
    return (int64_t)(avg[i] * 1000.0);
}

size_t auv_os_getenv(const char *name, char *buf, size_t cap) {
    char stack[4096];
    size_t n = sizeof(stack);
    if (uv_os_getenv(name, stack, &n) != 0)
        return 0;
    return auv__copy_cstr_n(stack, n, buf, cap);
}

int32_t auv_os_setenv(const char *name, const char *value) { return uv_os_setenv(name, value); }
int32_t auv_os_unsetenv(const char *name) { return uv_os_unsetenv(name); }

int32_t auv_os_getppid(void) { return (int32_t)uv_os_getppid(); }

int32_t auv_os_getpriority(int32_t pid, int32_t *out) {
    int prio = 0;
    int r = uv_os_getpriority((uv_pid_t)pid, &prio);
    if (r == 0)
        *out = (int32_t)prio;
    return r;
}

int32_t auv_os_setpriority(int32_t pid, int32_t prio) {
    return uv_os_setpriority((uv_pid_t)pid, prio);
}

/* What a descriptor IS, which is how a program decides whether it is talking to a person: 1 tty,
 * 2 pipe, 3 file, 4 tcp, 5 udp, 0 anything else. Mapped to numbers of this binding's own rather
 * than uv's enum, which has values this file would otherwise have to promise not to change. */
int32_t auv_guess_handle(int32_t fd) {
    switch (uv_guess_handle(fd)) {
    case UV_TTY:        return 1;
    case UV_NAMED_PIPE: return 2;
    case UV_FILE:       return 3;
    case UV_TCP:        return 4;
    case UV_UDP:        return 5;
    default:            return 0;
    }
}

/* Cryptographically strong bytes, from the platform's own source. The synchronous form: libuv
 * offers a threadpool one, and for the sizes anything here would ask for (a nonce, a key, a
 * token) the read is not worth a task park. */
int32_t auv_random(void *buf, size_t n) { return uv_random(NULL, NULL, buf, n, 0, NULL); }

/* Every network interface as one line of "<i|e><name>\t<address>", the same newline-separated
 * shape a directory listing uses, with the leading byte saying whether the interface is
 * internal (loopback) the way a dirent's says whether the entry is a directory. */
size_t auv_os_interfaces(char *buf, size_t cap) {
    uv_interface_address_t *ifs = NULL;
    int count = 0;
    if (uv_interface_addresses(&ifs, &count) != 0)
        return 0;

    size_t len = 0;
    for (int i = 0; i < count; i++) {
        char text[INET6_ADDRSTRLEN + 1];
        size_t an = auv__addr_text((const struct sockaddr *)&ifs[i].address, text, sizeof(text));
        if (an == 0)
            continue;
        size_t nn = strlen(ifs[i].name);
        size_t need = 1 + nn + 1 + an + 1;
        if (buf && len + need <= cap) {
            buf[len] = ifs[i].is_internal ? 'i' : 'e';
            memcpy(buf + len + 1, ifs[i].name, nn);
            buf[len + 1 + nn] = '\t';
            memcpy(buf + len + 2 + nn, text, an);
            buf[len + 2 + nn + an] = '\n';
        }
        len += need;
    }
    uv_free_interface_addresses(ifs, count);
    return len;
}
