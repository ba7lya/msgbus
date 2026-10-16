# ba7lya.msgbus

<!-- SPDX-License-Identifier: MIT -->

An in-process, hierarchical-topic message bus for an IoT gateway.

A gateway collects readings from many devices, normalises them, and hands them to whatever consumes
them — a logger, a protocol adapter, an uplink. `ba7lya.msgbus` is the piece in the middle: publishers
name a concrete topic, subscribers register a pattern, and the bus decides who hears what and on which
thread. It is header-only, C++23, and depends on nothing but the standard library and
[spdlog](https://github.com/gabime/spdlog) for diagnostics.

## Topics

A topic is a `/`-separated path of levels, as in `site/line1/device7/sensor/temperature`. A subscriber
registers a pattern over the same shape:

| Pattern level | Means |
|---|---|
| `site` | exactly this level |
| `+` | exactly one level, whatever it is |
| `#` | this level and every level below it; only valid as the last level |

Patterns are compiled into a trie, so resolution costs one step per level rather than one comparison
per subscription, and a topic with no live subscription under it is answered without a walk.

## The bus

```cpp
#include "msgbus.hxx"

using namespace ba7lya::msgbus;

msgbus bus;   // the routing worker starts with the bus

const auto id = bus.subscribe("site/+/device7/sensor/+", [](const message& msg) {
    // runs on the thread the subscription named
});

bus.publish_sync("site/line1/device7/sensor/temperature", std::any{std::uint64_t{23175}});
```

Two publish entry points, one routing path:

- `publish_sync` parses, resolves and delivers on the **calling thread** before returning, so a
  command being acknowledged or a shutdown path is complete when the call returns. Handlers that named
  an executor are handed over rather than waited on.
- `publish_async` hands the message to the bus's own routing worker and returns immediately.
  `ACCEPTED` is a promise: the message is routed even if the bus is released before the worker reaches
  it. `flush()` waits for that promise to be kept.

The payload is a `std::any`. Publisher and subscriber agree on a type without the bus knowing it — which
also means a subscriber that expects the wrong type is a contract the bus cannot check.

### Where handlers run

A subscription may name an executor. `dispatch_options::executor_` of `dispatcher::INLINE` (0) is the
default and runs the handler on the publishing thread; any other id must first be registered with
`bus.executors().add(id, capacity)`, and the handler then runs on that executor's own worker thread.
`dispatch_options::policy_` says what happens when that executor falls behind: `BLOCK`, `DROP_OLDEST`
or `DROP_NEW`.

A handler's exception is isolated from its siblings. When a caller is left to catch it — a synchronous
publish — it is thrown there; otherwise it goes to the subscription's `error_handler`, or, failing
that, to the bus's own `on_exception` sink, and if that is empty the failure is dropped rather than
allowed to unwind into a thread function.

## Tests, examples and benchmarks

All three are optional and each sits behind its own CMake option:

```console
$ cmake --preset linux-gcc-x64 \
    -Dba7lya.msgbus_BUILD_TEST=ON \
    -Dba7lya.msgbus_BUILD_BENCHMARK=ON \
    -Dba7lya.msgbus_BUILD_EXAMPLE=ON
$ cmake --build --preset linux-gcc-x64-dbg
$ ctest --preset linux-gcc-x64-test -C Debug
```

| Target | Source | What it is |
|---|---|---|
| `ba7lya.msgbus.ut` | `tests/ut/` | GoogleTest suite, one file per unit (`trie`, `layer_matcher`, `router`, `executor`, `dispatcher`, `msgbus`) |
| `msgbus_benchmark` | `benchmark/` | google benchmark: parse, matcher cycle/match, router resolve, sync fan-out, async round trip |
| `msgbus_example_basic_matching` | `examples/basic_matching.cxx` | two overlapping patterns, three publishes, and which subscriber saw what |

## Presets

`CMakePresets.json` defines the OS/compiler matrix, each with `dbg` and `rel` build presets and a
`-test` preset:

| Family | Presets | Architectures |
|---|---|---|
| Windows / MSVC | `win-msvc-2022-*`, `win-msvc-2026-*` | `x64`, `x86` |
| Linux / GCC | `linux-gcc-*` | `x64`, `x86` |
| Linux / Clang | `linux-clang-*` | `x64`, `x86` |

Each preset keeps its own vcpkg installed tree under `build/<preset>/vcpkg_installed`, and every one of
them requests the `test` and `benchmark` features declared in `vcpkg.json`.

### Sanitizer presets

A `<family>-<arch>-{asan,tsan,ubsan}` configure preset inherits its base and sets
`ba7lya.msgbus_SANITIZER`, which links an `INTERFACE` target carrying the instrumentation flags. That
target is deliberately not part of `ba7lya.msgbus` itself: this library is header-only, so anything on
its interface would propagate into every downstream consumer.

| Toolchain | address | thread | undefined |
|---|---|---|---|
| MSVC (`cl.exe`) | yes | no | no |
| GCC (Linux) | yes | yes | yes |
| Clang (Linux) | yes | yes | yes |

`cl.exe` **silently ignores** `/fsanitize=thread` and `/fsanitize=undefined` (warning `D9002`, exit code
0), so the build system fails loudly instead of producing a green build that checks nothing. Memory
sanitizer is absent everywhere: it needs every dependency instrumented, and vcpkg's ports are
precompiled. Thread sanitizer runs with the dependencies uninstrumented — the units under test
(`router`, `executor`, `dispatcher`) are instrumented, which is where the races would be.

## CI

`.github/workflows/ci.yml` runs four jobs:

- `win-msvc`, `linux-gcc`, `linux-clang` — Windows 2022 and 2026, GCC 14/15/16, Clang 21/22/23, each in
  Debug and Release, each also instrumented (address on Windows, address/thread/undefined on Linux),
  with configure, build, CTest, and a smoke run of the benchmark and one example.
- `clang-format` and `clang-tidy` — lint over `include tests examples benchmark`, pinned to LLVM 23.

Dependencies come from vcpkg, pinned by `VCPKG_COMMIT`; the vcpkg submodule is never checked out, since
`vcpkg.json`'s `builtin-baseline` already pins every port version.

`.github/workflows/nightly.yml` covers the axis a push-triggered run cannot: it takes vcpkg's current
default branch rather than `VCPKG_COMMIT`, and sets `VCPKG_BINARY_SOURCES: clear` so every port is
built from source rather than restored from the cache. Three jobs — Windows/MSVC 2026, the newest
GCC, and the newest Clang under thread sanitizer — answer "would a fresh machine still build this
tree?" the night before a baseline bump would discover the same thing. A port builds with a different
toolchain on Windows than on Linux, so all three are needed to say that.

## Layout

```
include/      the library: flat headers, included by bare filename
tests/ut/     unit tests
benchmark/    google benchmark suite
examples/     buildable examples
thirdparty/   the vcpkg submodule
```

## License

MIT. Every source file carries an `SPDX-License-Identifier: MIT` header.
