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
static size_t auv__copy_cstr(const char *s, char *buf, size_t cap) {
    if (!s)
        return 0;
    size_t n = strlen(s);
    if (buf && cap) {
        size_t k = n < cap ? n : cap;
        memcpy(buf, s, k);
    }
    return n;
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

int32_t auv_tcp_bind(uv_handle_t *h, const char *ip, int32_t port) {
    struct sockaddr_in addr;
    int r = uv_ip4_addr(ip, port, &addr);
    if (r != 0)
        return r;
    return uv_tcp_bind((uv_tcp_t *)h, (const struct sockaddr *)&addr, 0);
}

/* The port actually bound, which is what a bind to port 0 is for. */
int32_t auv_tcp_port(uv_handle_t *h) {
    struct sockaddr_storage ss;
    int len = (int)sizeof(ss);
    if (uv_tcp_getsockname((const uv_tcp_t *)h, (struct sockaddr *)&ss, &len) != 0)
        return -1;
    if (ss.ss_family != AF_INET)
        return -1;
    return (int32_t)ntohs(((struct sockaddr_in *)&ss)->sin_port);
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
    struct sockaddr_in addr;
    int r = uv_ip4_addr(ip, port, &addr);
    if (r != 0)
        return r;
    auv_creq *c = calloc(1, sizeof(auv_creq));
    if (!c)
        return UV_ENOMEM;
    c->slot = slot;
    r = uv_tcp_connect(&c->req, (uv_tcp_t *)h, (const struct sockaddr *)&addr, auv__on_connect);
    if (r != 0)
        free(c);
    return r;
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
        if (f->op == AUV_FS_READ) {
            payload = f->buf;      /* the slot owns the buffer from here */
            f->buf = NULL;
            aux = status;
        } else if (f->op == AUV_FS_STAT) {
            size_t n = 0;
            payload = auv__marshal_stat(&req->statbuf, &n);
            aux = (int64_t)n;
        } else if (f->op == AUV_FS_SCANDIR) {
            size_t n = 0;
            payload = auv__marshal_scandir(req, &n);
            aux = (int64_t)n;
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
    default: break;
    }

    if (r != 0) {
        free(f->buf);
        free(f);
    }
    return r;
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
            int64_t *out = malloc(AUV_STAT_FIELDS * sizeof(int64_t));
            if (!out)
                return UV_ENOMEM;
            out[0] = (int64_t)st.st_size;
            out[1] = (int64_t)st.st_mode;
            out[2] = S_ISDIR(st.st_mode) ? 1 : 0;
            out[3] = (int64_t)st.st_mtim.tv_sec;
            payload = out;
            aux = AUV_STAT_FIELDS * sizeof(int64_t);
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

/* `in_pipe`/`out_pipe`/`err_pipe` are initialized uv_pipe_t handles, or NULL for a stream the
 * child should not get. Returns the process handle, or NULL with the uv error in *err. */
uv_handle_t *auv_process_spawn(uv_loop_t *loop, const char *file, char **args, const char *cwd,
                               uv_handle_t *in_pipe, uv_handle_t *out_pipe, uv_handle_t *err_pipe,
                               int32_t *err) {
    uv_handle_t *h = auv__alloc_handle(UV_PROCESS);
    if (!h) {
        if (err)
            *err = UV_ENOMEM;
        return NULL;
    }

    uv_stdio_container_t stdio[3];
    memset(stdio, 0, sizeof(stdio));

    if (in_pipe) {
        stdio[0].flags = UV_CREATE_PIPE | UV_READABLE_PIPE;
        stdio[0].data.stream = (uv_stream_t *)in_pipe;
    } else {
        stdio[0].flags = UV_IGNORE;
    }
    if (out_pipe) {
        stdio[1].flags = UV_CREATE_PIPE | UV_WRITABLE_PIPE;
        stdio[1].data.stream = (uv_stream_t *)out_pipe;
    } else {
        stdio[1].flags = UV_IGNORE;
    }
    if (err_pipe) {
        stdio[2].flags = UV_CREATE_PIPE | UV_WRITABLE_PIPE;
        stdio[2].data.stream = (uv_stream_t *)err_pipe;
    } else {
        stdio[2].flags = UV_IGNORE;
    }

    uv_process_options_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.exit_cb = auv__on_exit;
    opts.file = file;
    opts.args = args;
    opts.cwd = cwd;
    opts.stdio_count = 3;
    opts.stdio = stdio;

    int r = uv_spawn(loop, (uv_process_t *)h, &opts);
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

int32_t auv_process_kill(uv_handle_t *h, int32_t signum) {
    return uv_process_kill((uv_process_t *)h, signum);
}

int32_t auv_process_pid(uv_handle_t *h) { return (int32_t)uv_process_get_pid((uv_process_t *)h); }
