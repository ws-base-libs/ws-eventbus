// ws-eventbus — correlation tracking.
//
// The point of a correlation id is that a request's events group MECHANICALLY.
// A handler must be able to ask "which request am I serving?" and get the truth
// without every signature carrying an id, and chained publishes must inherit it.
#include <ws/eventbus/eventbus.h>

#include <chrono>
#include <latch>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace {

struct Step {
    std::string name;
};

using ws::core::bus::Bus;
using ws::core::bus::CorrelationId;

}  // namespace

// --- a handler can read the id of the request it is servicing ----------------
TEST(Correlation, HandlerSeesTheCorrelationId) {
    Bus bus;
    std::optional<CorrelationId> seen;

    auto sub = bus.subscribe<Step>([&](Step const&) { seen = ws::core::bus::current_correlation_id(); });

    CorrelationId const id{"req-42"};
    bus.publish(Step{"start"}, id);

    for (int i = 0; i < 200 && !seen; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(seen.has_value());
    EXPECT_EQ(*seen, "req-42");
}

// --- a publish with no id still gets one, and it is unique -------------------
TEST(Correlation, GeneratedIdsAreUnique) {
    CorrelationId const a = ws::core::bus::generate_correlation_id();
    CorrelationId const b = ws::core::bus::generate_correlation_id();
    CorrelationId const c = ws::core::bus::generate_correlation_id();
    EXPECT_NE(a, b);
    EXPECT_NE(b, c);
    EXPECT_FALSE(a.empty());
}

// --- chained publishes inherit the id ---------------------------------------
TEST(Correlation, ChainedPublishInheritsTheCorrelationId) {
    Bus bus;
    std::vector<CorrelationId> seen;
    std::mutex                 m;
    std::latch                 done{1};

    // The second subscription publishes onward — exactly what a system does when
    // it reacts to an event by announcing its own result.
    auto inner = bus.subscribe<Step>([&](Step const& s) {
        if (s.name == "second") {
            std::lock_guard lock{m};
            seen.push_back(ws::core::bus::current_correlation_id().value_or(""));
            done.count_down();
        }
    });
    auto outer = bus.subscribe<Step>([&](Step const& s) {
        if (s.name == "first") {
            bus.publish(Step{"second"});   // no id supplied: must inherit
        }
    });

    bus.publish(Step{"first"}, CorrelationId{"req-7"});

    done.wait();
    std::lock_guard lock{m};
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen.front(), "req-7");
}

// --- an explicit id wins over the ambient one --------------------------------
TEST(Correlation, ExplicitIdOverridesTheAmbientOne) {
    Bus bus;
    std::optional<CorrelationId> seen;
    std::latch                   done{1};

    auto sub = bus.subscribe<Step>([&](Step const& s) {
        if (s.name == "inner") {
            seen = ws::core::bus::current_correlation_id();
            done.count_down();
        }
    });
    auto outer = bus.subscribe<Step>([&](Step const& s) {
        if (s.name == "outer") {
            bus.publish(Step{"inner"}, CorrelationId{"deliberately-different"});
        }
    });

    bus.publish(Step{"outer"}, CorrelationId{"req-9"});
    done.wait();

    ASSERT_TRUE(seen.has_value());
    EXPECT_EQ(*seen, "deliberately-different");
}

// --- command handlers see it too --------------------------------------------
TEST(Correlation, CommandHandlerSeesTheCorrelationId) {
    Bus  bus;
    std::optional<CorrelationId> seen;
    ASSERT_TRUE(bus
                    .handle<Step>([&](Step const&) -> int {
                        seen = ws::core::bus::current_correlation_id();
                        return 1;
                    })
                    .has_value());

    ASSERT_TRUE(bus.send<int>(Step{"go"}, CorrelationId{"req-cmd"}).has_value());
    ASSERT_TRUE(seen.has_value());
    EXPECT_EQ(*seen, "req-cmd");
}

// --- the id does not leak across requests ------------------------------------
TEST(Correlation, AmbientIdIsClearedBetweenJobs) {
    Bus                          bus;
    std::vector<std::optional<CorrelationId>> seen;
    std::mutex                   m;
    std::latch                   done{2};

    auto sub = bus.subscribe<Step>([&](Step const&) {
        {
            std::lock_guard lock{m};
            seen.push_back(ws::core::bus::current_correlation_id());
        }
        done.count_down();
    });

    bus.publish(Step{"a"}, CorrelationId{"one"});
    bus.publish(Step{"b"}, CorrelationId{"two"});
    done.wait();

    std::lock_guard lock{m};
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_NE(seen[0], seen[1]);
    EXPECT_EQ(*seen[0], "one");
    EXPECT_EQ(*seen[1], "two");
}
