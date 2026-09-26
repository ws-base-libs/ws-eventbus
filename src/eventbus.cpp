// ws-eventbus — implementation.
//
// Only the pieces that must be shared across translation units live here; the
// typed surface (subscribe/publish/handle/send) is in the public header and
// type-erases down to the entry points below.
#include <ws/eventbus/eventbus.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <format>
#include <future>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace ws::core::bus {
namespace {

// ---------------------------------------------------------------------------
// Correlation state. Thread-local because handlers run on their own workers:
// each worker sets it for the duration of exactly one job, so a handler can ask
// "which request am I serving?" and get the truth without plumbing an id through
// every signature.
// ---------------------------------------------------------------------------
thread_local std::optional<CorrelationId> t_current_correlation;

struct CorrelationScope {
    explicit CorrelationScope(CorrelationId const& id) {
        previous_ = t_current_correlation;
        t_current_correlation = id;
    }
    ~CorrelationScope() { t_current_correlation = previous_; }

    CorrelationScope(CorrelationScope const&)            = delete;
    CorrelationScope& operator=(CorrelationScope const&) = delete;

    std::optional<CorrelationId> previous_;
};

auto id_prefix() -> std::uint64_t {
    static std::uint64_t const prefix = [] {
        std::random_device rd;
        return (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
    }();
    return prefix;
}

}  // namespace

auto generate_correlation_id() -> CorrelationId {
    static std::atomic<std::uint64_t> counter{0};
    return std::format("corr-{:016x}-{}", id_prefix(),
                       counter.fetch_add(1, std::memory_order_relaxed));
}

auto current_correlation_id() -> std::optional<CorrelationId> const& {
    return t_current_correlation;
}

namespace detail {

// ---------------------------------------------------------------------------
// Sink — one bounded queue and one worker.
//
// Per-subscriber, not per-system: a subscriber is the unit that can fall behind,
// so it is the unit that owns a queue and a policy. The queue is preallocated at
// subscribe time to the declared capacity, so pushing does not allocate a node —
// the only per-publish allocation is the payload the publisher already made.
// ---------------------------------------------------------------------------
struct Sink {
    struct Item {
        Payload      payload;
        CorrelationId correlation;
    };

    explicit Sink(std::size_t capacity, OverflowPolicy policy, CmdInvoke invoke)
        : capacity_(capacity == 0 ? 1 : capacity),
          policy_(policy),
          invoke_(std::move(invoke)),
          queue_() {
        queue_.resize(capacity_);   // preallocated ring; no push-time allocation
        worker_ = std::thread([this] { run(); });
    }

    ~Sink() { close(); }

    Sink(Sink const&)            = delete;
    Sink& operator=(Sink const&) = delete;

    void close() noexcept {
        {
            std::lock_guard lock{mutex_};
            if (closed_) {
                return;
            }
            closed_ = true;
        }
        space_.notify_all();
        ready_.notify_all();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    /// Enqueue, honouring the overflow policy. Returns false if the sink is
    /// closed (the item is then simply not delivered).
    bool post(Payload payload, CorrelationId correlation) {
        std::unique_lock lock{mutex_};
        while (!closed_ && size_ == capacity_) {
            if (policy_ == OverflowPolicy::DropOldest) {
                // Shed the oldest to make room: load drops, the publisher never
                // stalls, and the freshest state survives.
                head_ = (head_ + 1) % capacity_;
                --size_;
                break;
            }
            // Block: backpressure. The publisher waits here, which is exactly
            // what "the pressure propagates back to the producer" means.
            space_.wait(lock);
        }
        if (closed_) {
            return false;
        }

        std::size_t const slot = (head_ + size_) % capacity_;
        queue_[slot] = Item{std::move(payload), std::move(correlation)};
        ++size_;
        lock.unlock();
        ready_.notify_one();
        return true;
    }

    /// Enqueue and wait for the item to be serviced. Commands use this so the
    /// caller gets its typed reply while the handler still runs on the worker.
    auto call(Payload payload, CorrelationId correlation)
        -> std::expected<Payload, BusError> {
        auto shared = std::make_shared<std::promise<std::expected<Payload, BusError>>>();
        auto future = shared->get_future();

        {
            std::lock_guard lock{mutex_};
            if (closed_) {
                return std::unexpected(BusError{
                    .code    = BusErrorCode::Closed,
                    .message = std::string{"the bus is shut down"},
                    .source  = "ws::core::bus::detail::Sink::call",
                });
            }
            pending_calls_.push_back(shared);
        }

        if (!post(std::move(payload), std::move(correlation))) {
            shared->set_value(std::unexpected(BusError{
                .code    = BusErrorCode::Closed,
                .message = std::string{"the bus shut down before the command was serviced"},
                .source  = "ws::core::bus::detail::Sink::call",
            }));
        }
        return future.get();
    }

private:
    void run() noexcept {
        for (;;) {
            Item item;
            {
                std::unique_lock lock{mutex_};
                ready_.wait(lock, [this] { return closed_ || size_ > 0; });
                if (size_ == 0) {
                    if (closed_) {
                        return;
                    }
                    continue;
                }
                item = std::move(queue_[head_]);
                queue_[head_] = Item{};
                head_ = (head_ + 1) % capacity_;
                --size_;
                lock.unlock();
                space_.notify_one();
            }

            CorrelationScope scope{item.correlation};
            std::expected<Payload, BusError> result{Payload{}};
            try {
                result = invoke_(item.payload, item.correlation);
            } catch (std::exception const& e) {
                result = std::unexpected(BusError{
                    .code    = BusErrorCode::HandlerFailed,
                    .message = std::string{"handler threw: "}.append(e.what()),
                    .source  = "ws::core::bus::detail::Sink::run",
                });
            } catch (...) {
                result = std::unexpected(BusError{
                    .code    = BusErrorCode::HandlerFailed,
                    .message = std::string{"handler threw a non-std exception"},
                    .source  = "ws::core::bus::detail::Sink::run",
                });
            }

            std::shared_ptr<std::promise<std::expected<Payload, BusError>>> waiter;
            {
                std::lock_guard lock{mutex_};
                if (!pending_calls_.empty()) {
                    waiter = std::move(pending_calls_.front());
                    pending_calls_.pop_front();
                }
            }
            if (waiter) {
                waiter->set_value(std::move(result));
            }
        }
    }

    std::size_t     capacity_;
    OverflowPolicy  policy_;
    CmdInvoke       invoke_;
    std::deque<std::shared_ptr<std::promise<std::expected<Payload, BusError>>>>
                    pending_calls_;

    std::mutex      mutex_;
    std::condition_variable ready_;
    std::condition_variable space_;
    std::vector<Item> queue_;
    std::size_t       head_ = 0;
    std::size_t       size_ = 0;
    bool              closed_ = false;
    std::thread       worker_;
};

auto make_event_sink(std::size_t capacity, OverflowPolicy policy, EventInvoke invoke)
    -> std::shared_ptr<Sink> {
    auto typed = std::move(invoke);
    return std::make_shared<Sink>(
        capacity, policy,
        [typed = std::move(typed)](Payload const& p,
                                   CorrelationId const& c) -> std::expected<Payload, BusError> {
            typed(p, c);
            return std::expected<Payload, BusError>{Payload{}};
        });
}

auto make_command_sink(std::size_t capacity, CmdInvoke invoke) -> std::shared_ptr<Sink> {
    return std::make_shared<Sink>(capacity, OverflowPolicy::Block, std::move(invoke));
}

void sink_post(Sink& sink, Payload payload, CorrelationId correlation) {
    sink.post(std::move(payload), std::move(correlation));
}

auto sink_call(Sink& sink, Payload payload, CorrelationId correlation)
    -> std::expected<Payload, BusError> {
    return sink.call(std::move(payload), std::move(correlation));
}

void sink_close(Sink& sink) noexcept { sink.close(); }

}  // namespace detail

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

Subscription::Subscription(std::shared_ptr<detail::Sink> sink) noexcept
    : sink_(std::move(sink)) {}

Subscription::Subscription(Subscription&& other) noexcept : sink_(std::move(other.sink_)) {
    other.sink_.reset();
}

Subscription& Subscription::operator=(Subscription&& other) noexcept {
    if (this != &other) {
        reset();
        sink_ = std::move(other.sink_);
        other.sink_.reset();
    }
    return *this;
}

Subscription::~Subscription() { reset(); }

void Subscription::reset() noexcept {
    if (sink_) {
        detail::sink_close(*sink_);
        sink_.reset();
    }
}

Subscription::operator bool() const noexcept { return static_cast<bool>(sink_); }

// ---------------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------------

struct Bus::Impl {
    std::mutex mutex;
    // One queue per subscriber; a type may have many.
    std::unordered_map<std::type_index, std::vector<std::shared_ptr<detail::Sink>>> events;
    // One handler per command type, enforced at registration.
    std::unordered_map<std::type_index, std::shared_ptr<detail::Sink>> commands;
};

Bus::Bus() : impl_(std::make_unique<Impl>()) {}

Bus::~Bus() {
    // Drain and stop every worker. Order does not matter: no system is being
    // stopped here, only the transport that carries their messages.
    std::vector<std::shared_ptr<detail::Sink>> all;
    {
        std::lock_guard lock{impl_->mutex};
        for (auto& [_, sinks] : impl_->events) {
            all.insert(all.end(), sinks.begin(), sinks.end());
        }
        for (auto& [_, sink] : impl_->commands) {
            all.push_back(sink);
        }
        impl_->events.clear();
        impl_->commands.clear();
    }
    for (auto& sink : all) {
        detail::sink_close(*sink);
    }
}

void Bus::post_all(std::type_index const& type, detail::Payload payload,
                   CorrelationId const& correlation) {
    // Copy the target list under the lock, post outside it: a handler that
    // subscribes or unsubscribes from inside a handler must not deadlock against
    // the publisher.
    std::vector<std::shared_ptr<detail::Sink>> targets;
    {
        std::lock_guard lock{impl_->mutex};
        if (auto it = impl_->events.find(type); it != impl_->events.end()) {
            targets = it->second;
        }
    }
    for (auto& sink : targets) {
        detail::sink_post(*sink, payload, correlation);
    }
}

auto Bus::call_one(std::type_index const& type, detail::Payload payload,
                   CorrelationId const& correlation) -> std::expected<detail::Payload, BusError> {
    std::shared_ptr<detail::Sink> target;
    {
        std::lock_guard lock{impl_->mutex};
        if (auto it = impl_->commands.find(type); it != impl_->commands.end()) {
            target = it->second;
        }
    }
    if (!target) {
        return std::unexpected(BusError{
            .code    = BusErrorCode::NoHandler,
            .message = std::string{"no handler registered for this command type"},
            .source  = "ws::core::bus::Bus::send",
        });
    }
    return detail::sink_call(*target, std::move(payload), correlation);
}

void Bus::register_event(std::type_index const& type,
                         std::shared_ptr<detail::Sink> sink) {
    std::lock_guard lock{impl_->mutex};
    impl_->events[type].push_back(std::move(sink));
}

auto Bus::register_command(std::type_index const& type,
                           std::shared_ptr<detail::Sink> sink) -> std::expected<void, BusError> {
    std::lock_guard lock{impl_->mutex};
    if (impl_->commands.contains(type)) {
        detail::sink_close(*sink);
        return std::unexpected(BusError{
            .code    = BusErrorCode::DuplicateHandler,
            .message = std::string{"a handler is already registered for this command type"},
            .source  = "ws::core::bus::Bus::handle",
        });
    }
    impl_->commands.emplace(type, std::move(sink));
    return {};
}

}  // namespace ws::core::bus
