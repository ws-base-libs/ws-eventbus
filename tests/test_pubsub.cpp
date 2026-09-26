// ws-eventbus — publish/subscribe.
//
// What is proven here is the QUEUE CONTRACT: fan-out, per-subscriber ordering,
// the bounded queue, and what each overflow policy does to it.
#include <ws/eventbus/eventbus.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <latch>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace {

struct Ping {
    int         n;
    std::string tag;
};

// Records the interleaving of handler and publisher actions, so ordering claims
// are asserted on a total order rather than on wall-clock time.
class Trace {
public:
    void record(std::string s) {
        std::lock_guard lock{mutex_};
        entries_.push_back(std::move(s));
    }
    [[nodiscard]] auto entries() const -> std::vector<std::string> {
        std::lock_guard lock{mutex_};
        return entries_;
    }
    [[nodiscard]] auto index_of(std::string const& s) const -> int {
        std::lock_guard lock{mutex_};
        for (std::size_t i = 0; i < entries_.size(); ++i) {
            if (entries_[i] == s) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

private:
    mutable std::mutex   mutex_;
    std::vector<std::string> entries_;
};

}  // namespace

// --- fan-out: every subscriber gets every event ------------------------------
TEST(PubSub, FanOutReachesEverySubscriber) {
    ws::core::bus::Bus bus;
    std::atomic<int>   a{0};
    std::atomic<int>   b{0};

    auto s1 = bus.subscribe<Ping>([&](Ping const&) { ++a; });
    auto s2 = bus.subscribe<Ping>([&](Ping const&) { ++b; });

    bus.publish(Ping{1, "x"});

    // Delivery is asynchronous by design; wait for both, do not sleep.
    for (int i = 0; i < 200 && (a == 0 || b == 0); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(a.load(), 1);
    EXPECT_EQ(b.load(), 1);
}

// --- per-subscriber ordering: FIFO within one queue --------------------------
TEST(PubSub, EventsArriveInPublishOrderPerSubscriber) {
    ws::core::bus::Bus      bus;
    std::mutex              m;
    std::vector<int>        seen;

    auto sub = bus.subscribe<Ping>([&](Ping const& p) {
        std::lock_guard lock{m};
        seen.push_back(p.n);
    });

    constexpr int kCount = 200;
    for (int i = 0; i < kCount; ++i) {
        bus.publish(Ping{i, "seq"});
    }

    for (int i = 0; i < 400; ++i) {
        {
            std::lock_guard lock{m};
            if (static_cast<int>(seen.size()) == kCount) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    std::lock_guard lock{m};
    ASSERT_EQ(seen.size(), static_cast<std::size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        EXPECT_EQ(seen[static_cast<std::size_t>(i)], i);
    }
}

// --- DropOldest: the queue sheds the OLDEST, never the newest ----------------
TEST(PubSub, DropOldestShedsTheOldest) {
    ws::core::bus::Bus bus;
    std::latch         gate{1};
    std::latch         started{1};
    std::mutex         m;
    std::vector<int>   seen;

    // Capacity 2. The handler holds the first item, so two more can only be
    // queued by displacing what is already there.
    auto sub = bus.subscribe<Ping>(
        [&](Ping const& p) {
            if (p.n == 1) {
                started.count_down();
                gate.wait();   // hold the queue's head
            }
            std::lock_guard lock{m};
            seen.push_back(p.n);
        },
        /*queue_capacity=*/2, ws::core::bus::OverflowPolicy::DropOldest);

    bus.publish(Ping{1, "a"});
    // Wait until #1 has actually been taken off the queue and is inside the
    // handler. Without this the outcome depends on whether the worker got there
    // first, and the test would be measuring the scheduler.
    started.wait();
    bus.publish(Ping{2, "b"});
    bus.publish(Ping{3, "c"});   // must displace 2, not 3
    bus.publish(Ping{4, "d"});   // must displace 3, not 4

    gate.count_down();

    for (int i = 0; i < 400; ++i) {
        {
            std::lock_guard lock{m};
            if (!seen.empty() && seen.back() == 4) {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    std::lock_guard lock{m};
    // 1 (already in flight) plus at most 2 queued. Newest survives, oldest shed.
    ASSERT_FALSE(seen.empty());
    EXPECT_EQ(seen.front(), 1);
    EXPECT_EQ(seen.back(), 4);
    EXPECT_LE(seen.size(), 3u);
    EXPECT_EQ(std::count(seen.begin(), seen.end(), 2), 0);   // shed
}

// --- Block: nothing is lost, and the publisher genuinely waits ----------------
TEST(PubSub, BlockAppliesBackpressureAndLosesNothing) {
    ws::core::bus::Bus bus;
    Trace              trace;
    std::latch         started{1};
    std::latch         gate{1};

    auto sub = bus.subscribe<Ping>(
        [&](Ping const& p) {
            if (p.n == 1) {
                trace.record("handle-start:1");
                started.count_down();
                gate.wait();
                trace.record("handle-end:1");
            } else {
                trace.record("handle:" + std::to_string(p.n));
            }
        },
        /*queue_capacity=*/1, ws::core::bus::OverflowPolicy::Block);

    bus.publish(Ping{1, "a"});
    started.wait();   // the handler is inside #1 and holds the only slot

    // The worker already took #1 off the queue, so the queue is EMPTY here and
    // nothing would block. Fill it first: #2 lands in the single slot, and only
    // then does #3 have to wait. (The original version of this test published
    // #2 into an empty queue and asserted on backpressure that never happened.)
    bus.publish(Ping{2, "b"});
    std::thread publisher{[&] {
        bus.publish(Ping{3, "c"});   // queue is full -> this must WAIT
        trace.record("publish-done:3");
    }};

    gate.count_down();
    publisher.join();

    for (int i = 0; i < 400; ++i) {
        if (trace.index_of("handle:2") >= 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // The claim under test: publish #2 could not complete while the handler held
    // the queue's only slot. That is backpressure, asserted as an ordering —
    // never as a duration.
    int const end1     = trace.index_of("handle-end:1");
    int const done2    = trace.index_of("publish-done:3");
    ASSERT_GE(end1, 0);
    ASSERT_GE(done2, 0);
    EXPECT_LT(end1, done2);

    // And with Block, nothing was lost.
    EXPECT_GE(trace.index_of("handle:3"), 0);
}

// --- the queue is bounded ----------------------------------------------------
TEST(PubSub, QueueIsBounded) {
    ws::core::bus::Bus bus;
    std::latch         gate{1};
    std::atomic<int>   handled{0};

    auto sub = bus.subscribe<Ping>(
        [&](Ping const& p) {
            if (p.n == 0) {
                gate.wait();
            }
            ++handled;
        },
        /*queue_capacity=*/4, ws::core::bus::OverflowPolicy::DropOldest);

    bus.publish(Ping{0, "hold"});
    for (int i = 1; i <= 1000; ++i) {
        bus.publish(Ping{i, "flood"});
    }
    gate.count_down();

    for (int i = 0; i < 400 && handled < 5; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // One in flight + at most 4 queued. The flood must not grow the queue.
    EXPECT_LE(handled.load(), 5);
}

// --- unsubscribe via the RAII token -----------------------------------------
TEST(PubSub, DestroyingTheSubscriptionUnsubscribes) {
    ws::core::bus::Bus bus;
    std::atomic<int>   calls{0};

    {
        auto sub = bus.subscribe<Ping>([&](Ping const&) { ++calls; });
        ASSERT_TRUE(static_cast<bool>(sub));
        bus.publish(Ping{1, "a"});
        for (int i = 0; i < 200 && calls == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        ASSERT_EQ(calls.load(), 1);
    }

    bus.publish(Ping{2, "b"});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(calls.load(), 1);   // the handler must not be reachable any more
}

// --- reset() unsubscribes earlier than destruction ---------------------------
TEST(PubSub, ResetUnsubscribesImmediately) {
    ws::core::bus::Bus bus;
    std::atomic<int>   calls{0};
    auto               sub = bus.subscribe<Ping>([&](Ping const&) { ++calls; });

    bus.publish(Ping{1, "a"});
    for (int i = 0; i < 200 && calls == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_EQ(calls.load(), 1);

    sub.reset();
    EXPECT_FALSE(static_cast<bool>(sub));
    bus.publish(Ping{2, "b"});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(calls.load(), 1);
}
