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
//   current_context()           which request am I serving, and who asked?
//
// SHAPE OF THE CONTRACT
//   - Event and command types are OWNING and copyable. No views, no pointers:
//     payloads cross a queue and may later cross a process boundary (§5.8).
//   - Handlers run on the subscriber's own worker, never on the publisher's
//     thread. A slow handler cannot stall a publisher — it can only fill the
//     subscriber's bounded queue, at which point that subscriber's
//     OverflowPolicy decides (Block applies backpressure; DropOldest sheds load).
//   - Every message carries a MessageContext: a correlation id (WHICH request)
//     and a source (WHO sent it). Both are inherited across chained calls, so a
//     request's whole trail groups without anyone plumbing ids by hand — and a
//     failure names the sender instead of leaving you to read everyone's logs.
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
#include <format>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <typeindex>
#include <vector>
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
// Correlation and origin
// ---------------------------------------------------------------------------

/// Opaque request identity. Nothing inspects the contents; it is a grouping key.
using CorrelationId = std::string;

/// Which request a message belongs to, and WHO sent it (tech spec §5.3 puts both
/// on every message). `source` is the sender's name — a system name in a `ws::`
/// program, anything you like elsewhere.
///
/// It exists because a correlation id groups a request but does not tell you who
/// made it. "Which of these eight senders threw?" must be answerable from the
/// error alone, not by reading every system's logs.
struct MessageContext {
    CorrelationId correlation_id;
    std::string   source;   ///< who sent it; empty when unknown
};

/// The message context being serviced on THIS thread, if any. Set before a
/// handler runs and restored after, so it can never leak between requests.
/// Chained calls inherit it unless they are given one.
[[nodiscard]] auto current_context() -> std::optional<MessageContext> const&;

/// Convenience: the correlation id alone.
///
/// BY VALUE, deliberately: there is no per-thread `optional<CorrelationId>` to
/// hand back a reference to, and binding `const&` to a temporary here segfaulted
/// the correlation suite the moment it was written. `current_context()` is the
/// one that returns a reference.
[[nodiscard]] auto current_correlation_id() -> std::optional<CorrelationId>;

/// Convenience: who sent the message being serviced, if known.
[[nodiscard]] auto current_source() -> std::optional<std::string>;

/// A fresh, process-unique correlation id.
[[nodiscard]] auto generate_correlation_id() -> CorrelationId;

/// Fills `error.context` with the request id and sender so a bus failure is
/// traceable to WHO sent it — without polluting `message`, which stays a single
/// human sentence. Uses ws-error's `context` chain (v0.2.0).
[[nodiscard]] auto describe_context(MessageContext const& context) -> std::vector<std::string>;

// ---------------------------------------------------------------------------
// Queueing
// ---------------------------------------------------------------------------

/// What a subscriber's bounded queue does when it is full.
enum class OverflowPolicy {
    Block,       ///< the publisher waits. Nothing is lost; pressure propagates back.
    DropOldest,  ///< the oldest queued item is shed. Load drops; nothing blocks.
};

// ---------------------------------------------------------------------------
// Internals. Declared so the templates below can be written against them;
// defined in the library, where the concrete types live. Consumers never name
// anything in this namespace.
// ---------------------------------------------------------------------------
namespace detail {

struct Sink;   // incomplete on purpose

using Payload     = std::shared_ptr<void const>;
using EventInvoke = std::function<void(Payload const&, MessageContext const&)>;
using CmdInvoke =
    std::function<std::expected<Payload, BusError>(Payload const&, MessageContext const&)>;

[[nodiscard]] auto make_event_sink(std::size_t capacity, OverflowPolicy policy,
                                   EventInvoke invoke) -> std::shared_ptr<Sink>;
[[nodiscard]] auto make_command_sink(std::size_t capacity, CmdInvoke invoke)
    -> std::shared_ptr<Sink>;

void sink_post(Sink& sink, Payload payload, MessageContext context);
[[nodiscard]] auto sink_call(Sink& sink, Payload payload, MessageContext context)
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
    /// thread, in the order the events were published. It receives the WHOLE
    /// event and uses what it needs.
    template <typename EventT, typename Handler>
        requires std::invocable<Handler&, EventT const&>
    [[nodiscard]] auto subscribe(Handler&& handler,
                                 std::size_t queue_capacity = 64,
                                 OverflowPolicy policy = OverflowPolicy::Block)
        -> Subscription {
        auto invoke = [h = std::function<void(EventT const&)>(
                           std::forward<Handler>(handler))](detail::Payload const& p,
                                                            MessageContext const&) {
            h(*static_cast<EventT const*>(p.get()));
        };
        auto sink = detail::make_event_sink(queue_capacity, policy, std::move(invoke));
        register_event(std::type_index{typeid(EventT)}, sink);
        return Subscription{std::move(sink)};
    }

    /// Broadcast an event. The context — including WHO sent it — is inherited
    /// from the ambient one, so a chain of reactions keeps the original sender.
    template <typename EventT>
    void publish(EventT const& event) {
        auto const& ambient = current_context();
        publish(event, ambient ? *ambient
                               : MessageContext{generate_correlation_id(), {}});
    }

    /// Broadcast under an explicit context. Use this to name the sender and to
    /// carry a request across a boundary that has no ambient context.
    template <typename EventT>
    void publish(EventT const& event, MessageContext const& context) {
        post_all(std::type_index{typeid(EventT)}, std::make_shared<EventT>(event),
                 context);
    }

    /// Broadcast under an existing request's correlation id; the sender is
    /// inherited from the ambient context.
    template <typename EventT>
    void publish(EventT const& event, CorrelationId const& correlation) {
        auto const& ambient = current_context();
        publish(event,
                MessageContext{correlation, ambient ? ambient->source : std::string{}});
    }

    // ---- commands --------------------------------------------------------

    /// Register the one handler for `CommandT`. Fails if one already exists —
    /// ambiguity at dispatch time is a design error, so it is refused at
    /// registration time instead, where it is cheap to see.
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
                          MessageContext const& ctx) -> std::expected<detail::Payload, BusError> {
            try {
                return std::expected<detail::Payload, BusError>{
                    std::make_shared<Reply>(h(*static_cast<CommandT const*>(p.get())))};
            } catch (...) {
                return std::unexpected(BusError{
                    .code    = BusErrorCode::HandlerFailed,
                    .message = std::string{"handler threw while servicing a command"},
                    .source  = "ws::core::bus::Bus::send",
                    .context = describe_context(ctx),
                });
            }
        };

        return register_command(
            std::type_index{typeid(CommandT)},
            detail::make_command_sink(queue_capacity, std::move(invoke)));
    }

    /// Send a command to its single handler and wait for the typed reply.
    /// `ReplyT` is the explicit template argument; the command type is deduced.
    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const& command) -> std::expected<ReplyT, BusError> {
        auto const& ambient = current_context();
        return send<ReplyT>(command,
                            ambient ? *ambient
                                    : MessageContext{generate_correlation_id(), {}});
    }

    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const& command, MessageContext const& context)
        -> std::expected<ReplyT, BusError> {
        auto reply = call_one(std::type_index{typeid(CommandT)},
                              std::make_shared<CommandT>(command), context);
        if (!reply) {
            auto error = reply.error();
            for (auto& line : describe_context(context)) {
                error.context.push_back(std::move(line));
            }
            return std::unexpected(error);
        }
        return *static_cast<ReplyT const*>(reply.value().get());
    }

    template <typename ReplyT, typename CommandT>
    [[nodiscard]] auto send(CommandT const& command, CorrelationId const& correlation)
        -> std::expected<ReplyT, BusError> {
        auto const& ambient = current_context();
        return send<ReplyT>(
            command,
            MessageContext{correlation, ambient ? ambient->source : std::string{}});
    }

private:
    void post_all(std::type_index const& type, detail::Payload payload,
                  MessageContext const& context);
    [[nodiscard]] auto call_one(std::type_index const& type, detail::Payload payload,
                                MessageContext const& context)
        -> std::expected<detail::Payload, BusError>;
    void register_event(std::type_index const& type, std::shared_ptr<detail::Sink> sink);
    [[nodiscard]] auto register_command(std::type_index const& type,
                                        std::shared_ptr<detail::Sink> sink)
        -> std::expected<void, BusError>;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ws::core::bus
