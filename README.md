# ba7lya.msgbus

<!-- SPDX-License-Identifier: MIT -->

[![ci](https://github.com/ba7lya/msgbus/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ba7lya/msgbus/actions/workflows/ci.yml)
[![nightly](https://github.com/ba7lya/msgbus/actions/workflows/nightly.yml/badge.svg?branch=main)](https://github.com/ba7lya/msgbus/actions/workflows/nightly.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An in-process message bus for an IoT gateway: publishers name a topic path, subscribers name a pattern,
and the bus resolves one against the other — level by level, or by literal prefix.

A gateway collects readings from many devices, normalises them, and hands them to whatever consumes
them — a logger, a protocol adapter, an uplink. `ba7lya.msgbus` is the piece in the middle, and it also
decides *which thread* hears: every subscription can name an executor, so its handler runs inline with the
publisher or on a worker of its own. It is a static library, C++23, and depends on nothing but the standard
library and [spdlog](https://github.com/gabime/spdlog) for diagnostics.

## Topics

A topic is a `/`-separated path of levels, as in `site/line1/device7/sensor/temperature`. A subscriber
registers a pattern over the same shape, and two matching modes decide what that pattern means. The mode is
chosen when the bus is built, with `matcher_type`:

| `matcher_type` | A subscription hears | Wildcards |
|---|---|---|
| `LAYER` (default) | the topics its pattern spells, level by level | `+` and `#` |
| `PREFIX` | its own path and every topic below it | none; the pattern is a literal path |

Both modes compile patterns into a trie, so resolution costs one step per level rather than one comparison
per subscription, and a topic with no live subscription under it is answered without a walk.

Under `LAYER`, a pattern level means:

| Pattern level | Means |
|---|---|
| `site` | exactly this level |
| `+` | exactly one level, whatever it is |
| `#` | this level and every level below it; only valid as the last level |

So `site/#` hears `site` itself as well as everything under it, `site/+/device7` hears exactly one level
between the two, and `#` alone is "everything".

Under `PREFIX`, every level is literal and the pattern matches its own path too: `site` hears `site` as well
as `site/line1/device7`, and never `sitex`, which merely shares a spelling. A pattern containing `+` or `#`
is rejected with `WILDCARD_IN_TOPIC`, because there it would be a wildcard in a concrete path.

```cpp
msgbus layered;                                  // LAYER: "site/+/device7", "device/#", ...
msgbus prefixed(matcher_type::PREFIX);           // PREFIX: "site/line1" hears site/line1 and below
```

A published topic is a concrete path in both modes: publishing to one that contains `+` or `#` fails with
`WILDCARD_IN_TOPIC` -- `publish_sync` throws `std::invalid_argument`, `publish_async` hands it to the bus's
error sink.

## The bus

```cpp
#include "msgbus.hxx"

using namespace ba7lya::msgbus;

msgbus bus;   // the routing worker starts with the bus

auto registered = bus.subscribe("site/+/device7/sensor/+", [](const message& msg) {
    // runs on the thread the subscription named
});
if (!registered) { /* the pattern was rejected: see registered.error().code_ */ }

bus.publish_sync("site/line1/device7/sensor/temperature", std::any{ std::uint64_t{ 23175 } });
```

`msgbus` is the whole bus: a program constructs it, `publisher`, `subscriber` and `adapter` all take a
`msgbus&`, and a test double derives from it and overrides what the test drives. What it owns is `msgbus::impl` — the routing table, the executors, the worker — a nested type defined
in `src/msgbus.cxx` and named by nothing outside the library. **An application includes the headers
under `include/` and links `ba7lya.msgbus`**; `src/` is the library's own.

`subscribe` returns a `result<subscription, error>`: the `subscription` inside owns the registration and
cancels it when destroyed, so the result has to stay alive for as long as the subscriber should keep
hearing. A bare `bus.subscribe(...)` statement is therefore a subscription that unsubscribes at the end of
that line — keep the result, and call `registered->cancel()` (or let it fall out of scope) to stop.

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

bus_log(direction::PUBLISH_SYNC, "site/line1/device7/sensor/temperature", reading);
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

The logger itself is a handle rather than a fixed default: `set_logger(std::shared_ptr<spdlog::logger>)`
points the trace at a caller's own sink, and `set_logger(nullptr)` puts it back. That is the injection point
a test uses to capture the lines and a gateway uses to send them to its file.

### Where handlers run

A subscription may name an executor. `dispatch_options::executor_` of `dispatcher::INLINE` (0) is the
default and runs the handler on the publishing thread; any other id must first be registered with
`bus.add_executor(id, capacity)`, and the handler then runs on that executor's own worker thread, where
`bus.drain_executor(id)` is how a caller waits for it. `dispatch_options::policy_` says what happens when
that executor falls behind: `BLOCK`, `DROP_OLDEST` or `DROP_NEW`.

A handler's exception is isolated from its siblings. When a caller is left to catch it — a synchronous
publish — it is thrown there; otherwise it goes to the subscription's `error_handler`, or, failing
that, to the bus's own `on_exception` sink, and if that is empty the failure is dropped rather than
allowed to unwind into a thread function.

## Components

A gateway is made of components, and most of them are one half of a conversation: an adapter that sends,
a logger that receives, a rule that reads one topic and writes another. Handing such a component the bus
itself would give it both halves, so the library names the halves:

| Type | Holds | For |
|---|---|---|
| `publisher` | a bus and a source name | a component that only sends. Every message it builds is stamped with `source()` and the next number from a counter of its own. |
| `subscriber` | a bus and the defaults | a component that only receives. Every registration it makes shares one lifetime and inherits one `dispatch_options` and `error_handler`. |
| `adapter` | both halves | a component that does both. The halves are private and every call they offer is forwarded, so they cannot be bound to different buses or to two names. |

```cpp
#include "adapter.hxx"   // brings `publisher` and `subscriber` with it

adapter bridge(bus, "serial-bridge");              // one name for both halves

bridge.subscribe("cmd/+", on_command);             // the subscribe half: an id, not a handle
bridge.publish_sync("device/1/temp", reading);     // the publish half: stamped and counted
```

None of the three owns the bus, so the bus must outlive them. None is copyable, and neither `publisher`
nor `adapter` is movable: two objects named one source would fork one sequence, and a subscriber's
handles are bound to the bus that issued them.

None of them has a virtual function either, so a component may **inherit** one or **hold** one — the
choice is about which API the component's own type exposes, not about what works. `examples/pubsub.cxx`
runs both shapes side by side, and the receivers must agree on what they heard.

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
| `ba7lya.msgbus.ut` | `tests/ut/` | GoogleTest suite, one file per unit (`error`, `trie`, `layer_matcher`, `prefix_matcher`, `router`, `executor`, `subscription`, `msgbus`, `publisher`, `subscriber`, `adapter`) |
| `msgbus_benchmark` | `benchmark/` | google benchmark: parse, matcher cycle/match, router resolve, sync fan-out, async round trip |
| `msgbus_example_basic_matching` | `examples/basic_matching.cxx` | two overlapping patterns, three publishes, and which subscriber saw what |
| `msgbus_example_wildcards` | `examples/wildcards.cxx` | five overlapping patterns and a reach matrix checked against a probe bus |
| `msgbus_example_sync_and_async` | `examples/sync_and_async.cxx` | one subscription reached on two threads, and where a handler's unhandled failure ends up on each path |
| `msgbus_example_pubsub` | `examples/pubsub.cxx` | one object of every component: two sensors, two devices, a `notifier` and a `monitor` over one bus, with the shapes inherited versus held |

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
`ba7lya.msgbus_SANITIZER`, which links an `INTERFACE` target carrying the instrumentation flags. That target
is linked PRIVATEly by the library and by every executable: the library's own objects have to be instrumented
too — MSVC records the sanitizer in `#pragma detect_mismatch`, so an instrumented executable linked against
an uninstrumented library fails with LNK2038 — and PRIVATE keeps the flags out of a downstream consumer's
compile line.

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

`.github/workflows/ci.yml` runs five jobs:

- `win-msvc`, `linux-gcc`, `linux-clang` — Windows 2022 and 2026, GCC 14/15/16, Clang 21/22/23, each in
  Debug and Release, each also instrumented (address on Windows, address/thread/undefined on Linux),
  with configure, build, CTest, and a smoke run of the benchmark and one example.
- `clang-format` and `clang-tidy` — lint over `include src tests examples benchmark`, pinned to LLVM 23.

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
include/          the public API: the headers an application includes
src/              the implementation, compiled into the static library -- the bus, dispatcher, executor,
                  router, trie, matcher, the two matchers, route, pattern
tests/ut/         unit tests
benchmark/        google benchmark suite
examples/         buildable examples
uml/              the class diagram, as PlantUML source
thirdparty/       the vcpkg submodule
LICENSE           MIT, in full
```

`include/` is the whole public API, and `src/` is the library's own machinery: an application includes the
headers under `include/`, links `ba7lya.msgbus`, and never sees a component. The bus is one object whose
implementation is `msgbus::impl`, held behind a pointer and defined only under `src/` — so a header change
under `src/` is a library rebuild, not an application rebuild.

## License

MIT — see [LICENSE](LICENSE) for the full text. Every source file carries an
`SPDX-License-Identifier: MIT` header, and the manifest declares the same licence in its `license`
field.
