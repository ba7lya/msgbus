# ba7lya.msgbus

<!-- SPDX-License-Identifier: MIT -->

[![ci](https://github.com/ba7lya/msgbus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ba7lya/msgbus/actions/workflows/ci.yml)
[![nightly](https://github.com/ba7lya/msgbus/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/ba7lya/msgbus/actions/workflows/nightly.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

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

The bus resolves patterns with one of two matchers, chosen at construction:

| `matcher_type` | Subscription means | Wildcards |
|---|---|---|
| `LAYER` (default) | the topic the pattern spells, level by level | `+` and `#` as above |
| `PREFIX` | that path and every topic below it | none; `+` and `#` are rejected |

```cpp
msgbus layered;                                  // "site/+/device7"
msgbus prefixed(matcher_type::PREFIX);           // "site/line1" hears site/line1/...
msgbus custom(std::make_unique<my_matcher>());   // any `matcher` implementation
```

## The bus

```cpp
#include "msgbus.hxx"

using namespace ba7lya::msgbus;

msgbus bus;   // the routing worker starts with the bus

subscription id = bus.subscribe("site/+/device7/sensor/+", [](const message& msg) {
    // runs on the thread the subscription named
});
if (!id) { /* the pattern was rejected: see id.error().code_ */ }

bus.publish_sync("site/line1/device7/sensor/temperature", std::any{std::uint64_t{23175}});
```

`subscribe` returns a `subscription`: a move-only handle that owns the registration and cancels it
when destroyed. A subscription's lifetime is therefore the handle's — leave it as a bare statement and
it unsubscribes at the end of that line. Keep the handle for as long as the subscriber should keep
hearing, and call `cancel()` (or let it fall out of scope) to stop.

Two publish entry points, one routing path:

- `publish_sync` parses, resolves and delivers on the **calling thread** before returning, so a
  command being acknowledged or a shutdown path is complete when the call returns. Handlers that named
  an executor are handed over rather than waited on.
- `publish_async` hands the message to the bus's own routing worker and returns immediately.
  `ACCEPTED` means the message is queued, not that it has been routed: `flush()` is how a caller waits
  until the worker is idle. A message already accepted is still routed when the bus is released.

The payload is a `std::any`. Publisher and subscriber agree on a type without the bus knowing it — which
also means a subscriber that expects the wrong type is a contract the bus cannot check.

`include/logger.hxx` is the trace a gateway needs and the bus deliberately does not write itself: a
message that was routed is not a message that was printed, and only the caller knows which of its own
events is worth a line.

```cpp
#include "logger.hxx"

bus_log(direction::publish_sync, "site/line1/device7/sensor/temperature", reading);
// 2026-10-17 01:14:13.630 [19952] PUB(sync)  site/line1/device7/sensor/temperature  23175
// 2026-10-17 01:14:13.630 [19952] SUB        site/line1/device7/sensor/temperature  23175  (line-reader)
```

One line per event — timestamp, thread, direction, topic, payload — over spdlog. `direction` separates
`publish_sync` from `publish_async` because the pair answers with the thread field what neither answers
alone: a synchronous publish and the deliveries it produces share a thread id, an asynchronous one and
its deliveries do not.

Two switches, doing different jobs. At build time, `ba7lya.msgbus_BUILD_LOG` (`ON` by default) decides
whether the trace is compiled in at all: off, the header emits no spdlog call and not one call site
changes. At run time, `configure_logging(log_config{ .enabled_ = false })` puts spdlog's level at `off`,
so a deployment can silence a build that has the trace in it.

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
    -Dba7lya.msgbus_BUILD_EXAMPLE=ON \
    -Dba7lya.msgbus_BUILD_LOG=ON
$ cmake --build --preset linux-gcc-x64-dbg
$ ctest --preset linux-gcc-x64-test -C Debug
```

| Target | Source | What it is |
|---|---|---|
| `ba7lya.msgbus.ut` | `tests/ut/` | GoogleTest suite, one file per unit (`trie`, `layer_matcher`, `prefix_matcher`, `router`, `executor`, `dispatcher`, `msgbus`) |
| `msgbus_benchmark` | `benchmark/` | google benchmark: parse, matcher cycle/match, router resolve, sync fan-out, async round trip |
| `msgbus_example_basic_matching` | `examples/basic_matching.cxx` | two overlapping patterns, three publishes, and which subscriber saw what |
| `msgbus_example_wildcards` | `examples/wildcards.cxx` | five overlapping patterns and a reach matrix checked against a probe bus |
| `msgbus_example_sync_and_async` | `examples/sync_and_async.cxx` | one subscription reached on two threads, and where a handler's unhandled failure ends up on each path |

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
toolchain on Windows than on Linux, so all three are needed to say that. A failed scheduled run files
a single `nightly` issue rather than notifying nobody.

## Layout

```
include/      the library: flat headers, included by bare filename
tests/ut/     unit tests
benchmark/    google benchmark suite
examples/     buildable examples
thirdparty/   the vcpkg submodule
LICENSE       MIT, in full
```

## License

MIT — see [LICENSE](LICENSE) for the full text. Every source file carries an
`SPDX-License-Identifier: MIT` header, and the manifest declares the same licence in its `license`
field.
