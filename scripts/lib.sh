#!/usr/bin/env bash
# scripts/lib.sh -- shared plumbing for every build in this repo. Source, don't run:
#
#     . "$here/../../scripts/lib.sh"
#
# Every ante build here goes through ante_build so the checked-in cc shim (scripts/ccshim/cc)
# is always on PATH -- see that file for why.

AUV_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ANTE="$(command -v ante || true)"

# The standard ASan cc flags. A lane `export AUV_CC_FLAGS="$AUV_ASAN_CC_FLAGS"` before
# ante_build; leak detection stays off at run time, because this compiler emits no drop glue
# and the tree-wide baseline is therefore "leaks are expected".
AUV_ASAN_CC_FLAGS="-fsanitize=address -g"

# aminicoro_include -- absolute path of the compiler's `aminicoro` directory (it holds
# minicoro.h, which src/Uv/csrc/pc_coro.c includes). Derived from the `ante` binary itself
# rather than from any assumed checkout location: a cargo build puts it at
# <ante>/target/{debug,release}/ante, so the sibling `aminicoro` is two directories above the
# binary. Requires `ante` on PATH to resolve (through symlinks) into a real ante checkout --
# nothing here assumes WHERE that is.
aminicoro_include() {
    local exe root
    [ -n "${ANTE:-}" ] || {
        echo "lib.sh: no ante compiler on PATH (needed to locate aminicoro/minicoro.h)" >&2
        return 66
    }
    exe="$(readlink -f "$ANTE")"
    root="$(cd "$(dirname "$exe")/../.." 2>/dev/null && pwd)" || root=""
    if [ -z "$root" ] || [ ! -f "$root/aminicoro/minicoro.h" ]; then
        echo "lib.sh: cannot locate aminicoro/minicoro.h from ante at '$exe'" \
             "(expected an ante checkout with the binary at target/{debug,release}/ante)" >&2
        return 66
    fi
    printf '%s\n' "$root/aminicoro"
}

# auv_build_shim DIR -- compile the binding's two C files into DIR as auv.o and pc_coro.o.
#
# Every lane compiles these ITSELF rather than sharing one prebuilt pair, which is what makes
# the asan mode meaningful: an uninstrumented object linked into an instrumented binary would
# hide exactly the shim bugs that mode exists to catch (slot lifetime, handle memory, buffer
# ownership). They are small and the compile is a fraction of the ante build next to it.
auv_build_shim() {
    local dir="$1" inc
    inc="$(aminicoro_include)" || return $?
    cc ${AUV_CC_FLAGS:-} -O2 -I "$inc" -c "$AUV_ROOT/src/Uv/csrc/auv.c"     -o "$dir/auv.o"
    cc ${AUV_CC_FLAGS:-} -O2 -I "$inc" -c "$AUV_ROOT/src/Uv/csrc/pc_coro.c" -o "$dir/pc_coro.o"
}

# ante_build DIR [ANTE_FLAGS...] -- run `ante build [flags]` in DIR through the cc shim.
# (Build options must follow the subcommand; the compiler rejects them before it.)
# Per-lane cc flags (asan etc.) go in exported AUV_CC_FLAGS, read by the shim itself.
ante_build() {
    local dir="$1"; shift
    [ -n "$ANTE" ] || {
        echo "lib.sh: no ante compiler on PATH (build it with 'cargo build --no-default-features'" \
             "in an ante checkout, then put target/debug/ante on PATH)" >&2
        return 66
    }
    # AUV_REAL_CC must be resolved against the UNmodified PATH (expansions happen before the
    # PATH assignment takes effect in this simple command, so `command -v cc` is the real one).
    ( cd "$dir" && AUV_REAL_CC="$(command -v cc)" \
        PATH="$AUV_ROOT/scripts/ccshim:$PATH" "$ANTE" build "$@" )
}

# auv_link_flags DIR -- the link inputs an auv program needs, given a DIR holding the objects
# `auv_build_shim` produced. One place so a lane never has to remember the list.
auv_link_flags() {
    printf '%s\n' --link-search "$1" --link-lib :auv.o --link-lib :pc_coro.o --link-lib uv
}
