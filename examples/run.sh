#!/usr/bin/env bash
# Build and run one example.
#
#     ./examples/run.sh                       list the examples
#     ./examples/run.sh timers                build + run examples/src/Timers.an
#     ./examples/run.sh tcp-echo-server       ... and pass anything after the name to it
#
# All the examples live in ONE ante project, because an ante project compiles everything under
# its src/ and the compiler picks the entry point out of the result (`--bin`). So each example
# is a single self-contained file next to the others, the way a luvit example is a single .lua
# file, rather than a directory with its own manifest.
#
# src/Uv is a symlink to the library; the compiler follows it like a real directory.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
. "$here/../scripts/lib.sh"

# Timers.an <-> timers, TcpEchoServer.an <-> tcp-echo-server.
kebab_of() { sed -e 's/\.an$//' -e 's/\([a-z0-9]\)\([A-Z]\)/\1-\2/g' <<< "$1" | tr '[:upper:]' '[:lower:]'; }
module_of() {
    local part out=""
    IFS='-' read -ra parts <<< "$1"
    for part in "${parts[@]}"; do out+="${part^}"; done
    printf '%s.an\n' "$out"
}

# Anything under src/ with a top-level `main`, minus main.an itself. That skips Run.an, which
# is the examples' shared plumbing rather than an example.
is_example() { [ -f "$1" ] && grep -q '^main ' "$1"; }

examples() {
    local f
    for f in "$here"/src/*.an; do
        [ "$(basename "$f")" = "main.an" ] && continue
        is_example "$f" || continue
        kebab_of "$(basename "$f")"
    done
}

usage() {
    echo "usage: $(basename "$0") <example> [args...]"
    echo
    echo "examples:"
    examples | sed 's/^/  /'
}

[ $# -ge 1 ] || { usage; exit 1; }
name="$1"; shift
module="$(module_of "$name")"
is_example "$here/src/$module" || { echo "no such example: $name" >&2; echo >&2; usage >&2; exit 1; }

mkdir -p "$here/build"
auv_build_shim "$here/build"
mapfile -t link < <(auv_link_flags "$here/build")
ante_build "$here" --backend c "${link[@]}" --bin "$module"
mv "$here/auv-example" "$here/build/$name"
rm -f "$here/a.out.c"

exec "$here/build/$name" "$@"
