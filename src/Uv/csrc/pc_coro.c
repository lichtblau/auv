/* pc_coro.c -- a coroutine constructor that honors the requested machine-stack size.
 *
 * The Ante compiler links `aminicoro/minicoro.c` into every binary, and the only constructor it
 * exposes is:
 *
 *     mco_coro* mco_coro_init(void (*fn)(mco_coro*), void* user_data)
 *         -> mco_desc_init(fn, 0)   // 0 == MCO_DEFAULT_STACK_SIZE
 *
 * With `MCO_USE_VMEM_ALLOCATOR` undefined (it is), MCO_DEFAULT_STACK_SIZE is **56 KB**. So every
 * coroutine got 56 KB no matter what the caller asked for. A task whose body makes deep native
 * calls overflows that and silently corrupts the malloc'd stack block, taking the `mco_coro`
 * header at its bottom with it -- which is why Task.an's `spawn` takes a stack size at all.
 *
 * `mco_desc_init` / `mco_create` are minicoro's public API and are already linked (minicoro.c is
 * compiled with MINICORO_IMPL), so this needs no change to the compiler or its submodule: include
 * the header for the declarations and call them with a real size.
 *
 * `stack_size == 0` keeps minicoro's default, matching `mco_coro_init` exactly.
 *
 * The file keeps its original name because Task.an declares the symbol `pc_coro_init_sized`; both
 * came from the editor this binding was extracted from.
 */

#include <stddef.h>

#include "minicoro.h"

mco_coro *pc_coro_init_sized(void (*entry)(mco_coro *co), void *user_data, size_t stack_size) {
    mco_desc desc = mco_desc_init(entry, stack_size);
    desc.user_data = user_data;
    mco_coro *co = NULL;
    if (mco_create(&co, &desc) != MCO_SUCCESS) {
        return NULL;
    }
    return co;
}
