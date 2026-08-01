# auv -- libuv for Ante

[libuv](https://libuv.org) bindings for [Ante](https://github.com/jfecher/ante).

## Requirements

* `ante` in `PATH`
* minicoro assumed to be located next to ante
* libuv1-dev

## Build and test

```sh
./tests/uv/run.sh
```

`rush.sh asan` runs with address sanitization.

## Examples

Each example is one file under `examples/src/`, and `examples/run.sh` builds and runs one by
name:

```sh
./examples/run.sh # print list of tests
./examples/run.sh timers
./examples/run.sh fs-basic
./examples/run.sh watch-dir
./examples/run.sh spawn-child
./examples/run.sh resolve
./examples/run.sh sys-info
./examples/run.sh unix-socket
./examples/run.sh udp-echo
./examples/run.sh tcp-echo-server
./examples/run.sh tcp-echo-client
```
