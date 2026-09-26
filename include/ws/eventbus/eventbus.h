// ws-eventbus — typed pub/sub, command dispatch, and correlation tracking.
//
// The runtime spine of a `ws::` codebase (tech spec §5). Systems never call each
// other: they publish events (broadcast, informational, past-tense) and send
// commands (directed, actionable, exactly one handler). Everything else in the
// architecture — independence, deployment evolution, observability — rests on
// that rule being cheap to follow.
//
// WHAT YOU GET
//   subscribe<EventT>(handler)  typed fan-out; returns an RAII Subscription
//   publish(event)              delivers to every matching subscriber
//   handle<CommandT>(handler)   exactly one handler per command type
//   send<ReplyT>(command)       typed request/response
//   current_correlation_id()    which request am I serving?
//
// SHAPE OF THE CONTRACT
//   - Event and command types are OWNING and copyable. No views, no pointers:
//     payloads cross a queue and may later cross a process boundary (§5.8).
//   - Handlers run on the subscriber's own worker, never on the publisher's
//     thread. A slow handler cannot stall the publisher — it can only fill the
//     subscriber's bounded queue, at which point that subscriber's OverflowPolicy
//     decides what happens (Block applies backpressure; DropOldest sheds load).
//   - A correlation id threads through everything, so a request's events group
//     mechanically instead of by hand.
//
// DEPENDENCIES: the C++ standard library and `ws-error` (for Error<Code>).
// `ws-error` is composed, not vendored — it is a published library like any other.
//
// THE PUBLIC HEADER PATH (`include/ws/eventbus/eventbus.h`) AND THE CMAKE TARGET
// (`ws-eventbus`) ARE PERMANENT and do not change across version bumps.

#pragma once

#include <ws/error/error.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <typeindex>
#include <utility>

namespace ws::core::bus {

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

enum class BusErrorCode {
    NoHandler,         ///< `send` for a command type nothing has registered
    DuplicateHandler,  ///< a second `handle` for the same command type
    HandlerFailed,     ///< the handler threw; the exception never escapes the bus
    Closed,            ///< the bus or sink has shut down
};

using BusError = ws::Error<BusErrorCode>;

// ---------------------------------------------------------------------------
// Correlation
// ---------------------------------------------------------------------------

/// Opaque request identity. Nothing inspects the contents; it is a grouping key.
using CorrelationId = std::string;

/// A fresh, process-unique id.
[[nodiscard]] auto generate_correlation_id() -> CorrelationId;

/// The id of the request currently being serviced on THIS thread, if any.
/// Read it inside a handler; it is set before the handler runs and restored after.
/// Chained publishes inherit it unless they supply their own.
[[nodiscard]] auto current_correlation_id() -> std::optional<CorrelationId> const&;

// ---------------------------------------------------------------------------
// Queueing
// ---------------------------------------------------------------------------

/// What a subscriber's bounded queue does when it is full.
enum class OverflowPolicy {
    Block,       ///< the publisher waits. Nothing is lost; pressure propagates back.
    DropOldest,  ///< the oldest queued item is shed. Load drops; nothing blocks.
};

// ---------------------------------------------------------------------------
// Internals. Declared here so the templates below can be written against them;
// defined in the library, where the concrete types live. Consumers never name
// anything in this namespace.
// ---------------------------------------------------------------------------
namespace detail {

struct Sink;   // incomplete on purpose

using Payload     = std::shared_ptr<void const>;
using EventInvoke = std::function<void(Payload const&, CorrelationId const&)>;
using CmdInvoke =
    std::function<std::expected<Payload, BusError>(Payload const&, CorrelationId const&)>;

[[nodiscard]] auto make_event_sink(std::size_t capacity, OverflowPolicy policy,
                                   EventInvoke invoke) -> std::shared_ptr<Sink>;
[[nodiscard]] auto make_command_sink(std::size_t capacity, CmdInvoke invoke)
    -> std::shared_ptr<Sink>;

void sink_post(Sink& sink, Payload payload, CorrelationId correlation);
[[nodiscard]] auto sink_call(Sink& sink, Payload payload, CorrelationId correlation)
    -> std::expected<Payload, BusError>;
void sink_close(Sink& sink) noexcept;

}  // namespace detail

// ---------------------------------------------------------------------------
// Subscription — RAII. Destroying it unsubscribes.
// ---------------------------------------------------------------------------

class Subscription {
public:
    Subscription() = default;

    Subscription(Subscription const&)            = delete;
    Subscription& operator=(Subscription const&) = delete;
    Subscription(Subscription&&) noexcept;
    Subscription& operator=(Subscription&&) noexcept;

    /// Unsubscribes if still attached. Safe to call twice.
    ~Subscription();

    /// Explicit unsubscribe, earlier than destruction.
    void reset() noexcept;

    [[nodiscard]] explicit operator bool() const noexcept;

private:
    friend class Bus;
    explicit Subscription(std::shared_ptr<detail::Sink> sink) noexcept;

    std::shared_ptr<detail::Sink> sink_;
};

// ---------------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------------

class Bus {
public:
    Bus();
    ~Bus();

    Bus(Bus const&)            = delete;
    Bus& operator=(Bus const&) = delete;

    // ---- events ----------------------------------------------------------

    /// Subscribe to `EventT`. The handler runs on this subscriber's own worker
    /// thread, in the order the events were published.
    template <typename EventT, typename Handler>
        requires std::invocable<Handler&, EventT const&>
    [[nodiscard]] auto subscribe(Handler&& handler,
                                 std::size_t queue_capacity = 64,
                                 OverflowPolicy policy = OverflowPolicy::Block)
        -> Subscription {
        auto invoke = [h = std::function<void(EventT const&)>(
                           std::forward<Handler>(handler))](detail::Payload const& p,
                                                            CorrelationId const&) {
            h(*static_cast<EventT const*>(p.get()));
        };
        auto sink = detail::make_event_sink(queue_capacity, policy, std::move(invoke));
        register_event(std::type_index{typeid(EventT)}, sink);
        return Subscription{std::move(sink)};
    }

    /// Broadcast an event. Every matching subscriber receives its own copy.
    ///
    /// The correlation id is INHERITED from the ambient one when there is one —
    /// that is what makes a request's events group mechanically across a chain of
    /// reactions — and generated fresh otherwise. Supplied explicitly to carry a
    /// request across a boundary that does not have the ambient id.
    template <typename EventT>
    void publish(EventT const& event) {
        auto const& ambient = current_correlation_id();
        publish(event, ambient ? *ambient : generate_correlation_id());
    }

    /// Broadcast an event under an existing request's correlation id.
    template <typename EventT>
    void publish(EventT const& event, CorrelationId const& correlation) {
        auto payload = std::make_shared<EventT>(event);
        post_all(std::type_index{typeid(EventT)}, std::move(payload), correlation);
    }

    // ---- commands --------------------------------------------------------

    /// Register the one handler for `CommandT`. Fails if one already exists —
    /// ambiguity at dispatch time is a design error, so it is refused at
    /// registration time instead.
    template <typename CommandT, typename Handler>
    [[nodiscard]] auto handle(Handler&& handler, std::size_t queue_capacity = 64)
        -> std::expected<void, BusError> {
        using Reply = std::invoke_result_t<Handler&, CommandT const&>;
        static_assert(!std::is_void_v<Reply>,
                      "a command handler must return a reply; use publish for "
                      "fire-and-forget");

        auto invoke = [h = std::function<Reply(CommandT const&)>(
                           std::forward<Handler>(handler))](
                          detail::Payload const& p,
                          CorrelationId const&) -> std::expected<detail::Payload, BusError> {
            try {
                return std::expected<detail::Payload, BusError>{
                    std::make_shared<Reply>(h(*static_cast<CommandT const*>(p.get())))};
            } catch (...) {
                return std::unexpected(BusError{
                    .code    = BusErrorCode::HandlerFailed,
                    .message = std::string{"handler threw while servicing a command"},
                    .source  = "ws::core::bus::Bus::send",
                });
            }
        };

        return register_command(
            std::type_index{typeid(CommandT)},
            detail::make_command_sink(queue_capacity, std::move(invoke)));
    }

    /// Send a command to its single handler and wait for the typed reply.
    /// `ReplyT` is written explicitly; the command type is deduced from `command`.
    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const& command) -> std::expected<ReplyT, BusError> {
        auto const& ambient = current_correlation_id();
        return send<ReplyT>(command, ambient ? *ambient : generate_correlation_id());
    }

    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const& command, CorrelationId const& correlation)
        -> std::expected<ReplyT, BusError> {
        auto payload = std::make_shared<CommandT>(command);
        auto reply   = call_one(std::type_index{typeid(CommandT)}, std::move(payload),
                                correlation);
        if (!reply) {
            return std::unexpected(reply.error());
        }
        if constexpr (std::is_void_v<ReplyT>) {
            return {};
        } else {
            return *static_cast<ReplyT const*>(reply.value().get());
        }
    }

private:
    void post_all(std::type_index const& type, detail::Payload payload,
                  CorrelationId const& correlation);
    [[nodiscard]] auto call_one(std::type_index const& type, detail::Payload payload,
                                CorrelationId const& correlation)
        -> std::expected<detail::Payload, BusError>;
    void register_event(std::type_index const& type, std::shared_ptr<detail::Sink> sink);
    [[nodiscard]] auto register_command(std::type_index const& type,
                                        std::shared_ptr<detail::Sink> sink)
        -> std::expected<void, BusError>;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ws::core::bus
