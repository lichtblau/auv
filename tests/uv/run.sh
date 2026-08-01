#!/usr/bin/env bash
# Build + run the src/Uv test suite.
#
# The suite is its own ante project (name "uv-tests"): an ante project has exactly one main,
# so a test-runner main needs a project of its own. src/Uv is reached through the checked-in
# symlink src/Uv -> ../../../src/Uv, which the compiler follows like a real directory.
#
# Runs at BOTH optimization levels by default (see build_and_run below). Pass `asan` for an
# AddressSanitizer run, or `o3` for just the optimized one. Leak detection stays off at run
# time: this compiler emits no drop glue, so the tree-wide baseline is "leaks are expected".
#
# LEAK BASELINE, measured with ASAN_OPTIONS=detect_leaks=1 on 2026-07-29:
#   21525 bytes in 951 allocations, 66 distinct reports.
# All of it is Ante-side and expected: strings built by the suite (uv error names and
# messages, temp-directory paths), the test harness's own vectors, and the small wrapper cells
# a Stream and a Listener are made of, which are deliberately not reclaimed -- the handle and
# wait slot they point at ARE released, by libuv's close callback. NOTHING in the report comes
# from src/Uv/csrc/auv.c: no handle, request, read buffer or wait slot leaks. That is what a
# re-measurement should check. A new auv.c frame appearing in a leak stack is a real bug.
#
# Timing discipline: every ordering assertion uses coarse, well-separated durations (50 ms
# apart or more) and no test asserts a wall-clock duration.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
. "$here/../../scripts/lib.sh"
mode="${1:-plain}"

if [ "$mode" = "asan" ]; then
    export AUV_CC_FLAGS="$AUV_ASAN_CC_FLAGS"
fi

auv_build_shim "$here"

cleanup() { rm -f "$here/uv-tests" "$here/a.out.c" "$here/auv.o" "$here/pc_coro.o"; }
trap cleanup EXIT

# The suite runs at BOTH optimization levels by default, and that is the point rather than
# thoroughness for its own sake: a `Result T UvError` shape it exercised happily at the default
# level once segfaulted at -O3 (see the UvError comment in src/Uv/Uv.an). Two builds of a
# ten-second lane is a cheap way to stop that happening again.
#
# `asan` and `o3` run a single level each, for when you want one.
build_and_run() {
    local label="$1"; shift
    echo "==> uv lane: $label"
    mapfile -t link < <(auv_link_flags "$here")
    ante_build "$here" --backend c "$@" "${link[@]}"
    if [ "$mode" = "asan" ]; then
        ASAN_OPTIONS=detect_leaks=0 "$here/uv-tests"
    else
        "$here/uv-tests"
    fi
}

case "$mode" in
    asan) build_and_run "AddressSanitizer" ;;
    o3)   build_and_run "-O3" -O3 ;;
    *)    build_and_run "default optimization"
          build_and_run "-O3" -O3 ;;
esac
