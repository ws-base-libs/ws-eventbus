# ws-eventbus

**Typed pub/sub, command dispatch, and correlation tracking for C++23.** The
runtime spine of a `ws::` codebase: systems never call each other, they exchange
data on a bus.

Header + static library. Dependencies: the C++ standard library and
[`ws-error`](https://github.com/ws-base-libs/ws-error). Nothing else.

---

## Why

Modular systems usually end up coupled anyway: one calls another's method, and
now adding a third system means editing the first two. A bus inverts that. A
system says what *happened* (an event) or *asks for something* (a command), and
whoever cares reacts. Adding a system touches nobody.

The other half is correlation: once messages stop flowing through direct calls,
you lose the thread of *which request caused this*. A correlation id threaded
through every message gets it back — mechanically, instead of by hand.

## What you get

| | |
|---|---|
| `subscribe<EventT>(handler)` | typed fan-out; returns an RAII `Subscription` |
| `publish(event)` | delivers to every matching subscriber |
| `handle<CommandT>(handler)` | exactly one handler per command type |
| `send<ReplyT>(command)` | typed request/response |
| `current_correlation_id()` | which request am I serving? |

## Usage

### Events — broadcast, informational, past-tense

```cpp
#include <ws/eventbus/eventbus.h>

struct ModelLoaded { std::string name; std::size_t context_size; };

ws::core::bus::Bus bus;

// RAII: destroying the subscription unsubscribes.
auto sub = bus.subscribe<ModelLoaded>([](ModelLoaded const& e) {
    std::println("model {} is resident", e.name);
});

bus.publish(ModelLoaded{.name = "small", .context_size = 4096});
```

### Commands — directed, imperative, exactly one handler

```cpp
struct LoadModel  { std::string path; };
struct LoadReply  { bool ok; std::string handle; };

auto registered = bus.handle<LoadModel>([](LoadModel const& cmd) -> LoadReply {
    return LoadReply{.ok = true, .handle = open(cmd.path)};
});

auto reply = bus.send<LoadReply>(LoadModel{.path = "/models/small.gguf"});
if (!reply) {
    log(reply.error().code, reply.error().message.value_or(""));
}
```

A second `handle` for the same type is **refused at registration**, and a `send`
with no handler returns `BusErrorCode::NoHandler` — ambiguity is a design error,
so it is caught where it is introduced rather than at 3am.

### Correlation — grouping a request's events

```cpp
bus.subscribe<DecodeStep>([](DecodeStep const& step) {
    auto const& id = ws::core::bus::current_correlation_id();
    metrics.record(*id, step);
});

bus.publish(DecodeStep{...}, request_id);   // explicit: carries the id in
bus.publish(DecodeStep{...});               // inherited: keeps the current one
```

The id is **inherited** across chained calls — a handler that publishes keeps the
id of the request it is servicing — so a request's whole event trail groups
without any plumbing. Supply an explicit id to carry a request across a boundary
that does not have the ambient one.

## Queueing and backpressure

Every subscriber has its **own bounded queue** and its **own worker thread**. A
handler never runs on the publisher's thread, so a slow handler cannot stall a
producer — it can only fill its own queue, and then its `OverflowPolicy` decides:

| Policy | When full | Use for |
|---|---|---|
| `Block` (default) | the publisher waits | things that must not be lost |
| `DropOldest` | the oldest queued item is shed | advisory/state where freshest wins |

```cpp
bus.subscribe<CacheStatus>(handler,
                           /*queue_capacity=*/32,
                           ws::core::bus::OverflowPolicy::DropOldest);
```

`Block` is what "backpressure propagates back to the producer" actually means:
the producer is made to wait by the consumer that cannot keep up.

## Errors

Everything fallible returns `std::expected<T, ws::Error<BusErrorCode>>`:

| Code | Meaning |
|---|---|
| `NoHandler` | `send` for a command type nothing registered |
| `DuplicateHandler` | a second `handle` for the same command type |
| `HandlerFailed` | the handler threw — the exception never escapes the bus |
| `Closed` | the bus or a sink has shut down |

A throwing handler is converted to `HandlerFailed` with a `source`, not
propagated into the caller's stack. That is deliberate: one bad handler must not
be able to take a caller down.

## API reference

```cpp
namespace ws::core::bus {

using CorrelationId = std::string;
using BusError      = ws::Error<BusErrorCode>;
enum class BusErrorCode { NoHandler, DuplicateHandler, HandlerFailed, Closed };
enum class OverflowPolicy { Block, DropOldest };

[[nodiscard]] auto generate_correlation_id() -> CorrelationId;
[[nodiscard]] auto current_correlation_id() -> std::optional<CorrelationId> const&;

class Subscription {          // move-only, RAII
    void reset() noexcept;    // unsubscribe early
    explicit operator bool() const noexcept;
};

class Bus {
    Bus();  ~Bus();  Bus(Bus const&) = delete;  Bus& operator=(Bus const&) = delete;

    template <typename EventT, typename Handler>
    [[nodiscard]] auto subscribe(Handler&&, std::size_t capacity = 64,
                                 OverflowPolicy = OverflowPolicy::Block) -> Subscription;

    template <typename EventT> void publish(EventT const&);
    template <typename EventT> void publish(EventT const&, CorrelationId const&);

    template <typename CommandT, typename Handler>
    [[nodiscard]] auto handle(Handler&&, std::size_t capacity = 64)
        -> std::expected<void, BusError>;

    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const&) -> std::expected<ReplyT, BusError>;
    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const&, CorrelationId const&)
        -> std::expected<ReplyT, BusError>;
};

}
```

`send<ReplyT>` takes the reply type explicitly; the command type is deduced from
the argument.

## Build and test

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Tests build only when this is the top-level project, so consuming it through
FetchContent never drags our test binaries — or our GoogleTest — into your build.

```cmake
FetchContent_Declare(ws-eventbus GIT_REPOSITORY <this repo> GIT_TAG v0.1.0)
FetchContent_MakeAvailable(ws-eventbus)
target_link_libraries(your_target PRIVATE ws-eventbus)
```

## Layout

```
include/ws/eventbus/eventbus.h   the public header — THIS PATH IS PERMANENT
src/eventbus.cpp                 sinks, workers, correlation state
src/details/                     internal headers
tests/                           standalone suites (pubsub / command / correlation)
```

`include/ws/eventbus/eventbus.h` and the CMake target `ws-eventbus` will not
change: the include statement and the `target_link_libraries` line above are
stable across every version bump.

## Design notes

- **Event and command types are owning and copyable.** No views, no pointers.
  Payloads cross a queue and may later cross a process boundary (§5.8), so a
  payload that points into a caller's stack is a bug waiting for its second
  deployment.
- **One queue per subscriber**, not per system: the subscriber is the unit that
  can fall behind, so it is the unit that owns a queue and a policy.
- **The queue is preallocated** to its declared capacity, so pushing does not
  allocate a node. The one remaining per-publish allocation is the payload copy.
- **`handle` refuses duplicates** rather than picking one. `send` returns
  `NoHandler` rather than throwing.
- **Correlation is thread-local and scoped to one job.** It is set before a
  handler runs and restored after, so it can never leak from one request into
  the next.

## Extension points

The bus is deliberately small and complete. Before extending it, consider:

- **New delivery semantics** (topics/wildcards, priorities, request timeouts) —
  propose upstream; they belong in the bus, not in a wrapper.
- **A different transport** — the `Sink` boundary is where in-process delivery is
  decided. Swapping it for a socket is the intended seam for a process-boundary
  bus, and does not change this API.
- **Your own queueing** — `OverflowPolicy` is a closed enum on purpose. If you
  need something else, it is a policy worth having for everyone.

Resist adding project-specific concepts to this library. It is meant to be
usable in any C++23 project, so anything domain-flavoured belongs in the system
that owns it.

## Licence

To be added by the owning organisation.
