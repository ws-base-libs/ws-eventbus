// ws-eventbus — command dispatch.
//
// Commands are directed and imperative: exactly ONE handler per type, typed
// request/response, and a handler that throws must NOT blow up the caller.
#include <ws/eventbus/eventbus.h>

#include <expected>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

namespace {

struct LoadModel {
    std::string path;
};

struct LoadModelReply {
    bool        ok;
    std::string handle;
};

struct Boom {};

using ws::core::bus::Bus;
using ws::core::bus::BusErrorCode;

}  // namespace

// --- the basic round trip ----------------------------------------------------
TEST(Command, ReplyRoundTrip) {
    Bus bus;
    auto registered = bus.handle<LoadModel>([](LoadModel const& cmd) -> LoadModelReply {
        return LoadModelReply{.ok = true, .handle = "h:" + cmd.path};
    });
    ASSERT_TRUE(registered.has_value());

    auto reply = bus.send<LoadModelReply>(LoadModel{.path = "/m/gguf"});
    ASSERT_TRUE(reply.has_value()) << (reply ? "" : reply.error().message.value_or(""));
    EXPECT_TRUE(reply->ok);
    EXPECT_EQ(reply->handle, "h:/m/gguf");
}

// --- exactly one handler per command type -----------------------------------
TEST(Command, DuplicateHandlerIsRefusedAtRegistration) {
    Bus bus;
    ASSERT_TRUE(bus.handle<LoadModel>([](LoadModel const&) { return LoadModelReply{}; })
                    .has_value());

    auto second = bus.handle<LoadModel>([](LoadModel const&) { return LoadModelReply{}; });
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error().code, BusErrorCode::DuplicateHandler);
}

// --- an unhandled command is an error, not a crash ---------------------------
TEST(Command, UnknownCommandIsAnErrorNotACrash) {
    Bus  bus;
    auto reply = bus.send<LoadModelReply>(LoadModel{.path = "/m"});
    ASSERT_FALSE(reply.has_value());
    EXPECT_EQ(reply.error().code, BusErrorCode::NoHandler);
}

// --- a throwing handler never escapes ---------------------------------------
TEST(Command, HandlerExceptionIsConvertedToAnError) {
    Bus bus;
    auto registered = bus.handle<Boom>([](Boom const&) -> int {
        throw std::runtime_error{"handler exploded"};
    });
    ASSERT_TRUE(registered.has_value());

    auto reply = bus.send<int>(Boom{});
    ASSERT_FALSE(reply.has_value());
    EXPECT_EQ(reply.error().code, BusErrorCode::HandlerFailed);
    // §2.7: the error must be traceable to its origin.
    EXPECT_TRUE(reply.error().source.has_value());
}

// --- a non-std exception is contained too ------------------------------------
TEST(Command, NonStdExceptionIsConvertedToAnError) {
    Bus bus;
    ASSERT_TRUE(bus.handle<Boom>([](Boom const&) -> int { throw 42; }).has_value());

    auto reply = bus.send<int>(Boom{});
    ASSERT_FALSE(reply.has_value());
    EXPECT_EQ(reply.error().code, BusErrorCode::HandlerFailed);
}

// --- the handler runs on the bus's worker, not the caller's thread ------------
TEST(Command, HandlerRunsOnItsOwnWorker) {
    Bus          bus;
    std::thread::id handler_thread;
    ASSERT_TRUE(bus
                    .handle<LoadModel>([&](LoadModel const&) -> LoadModelReply {
                        handler_thread = std::this_thread::get_id();
                        return LoadModelReply{};
                    })
                    .has_value());

    auto const caller = std::this_thread::get_id();
    ASSERT_TRUE(bus.send<LoadModelReply>(LoadModel{}).has_value());
    EXPECT_NE(handler_thread, caller);
}

// --- typed replies are really typed -----------------------------------------
TEST(Command, ReplyTypeIsPreserved) {
    Bus bus;
    ASSERT_TRUE(bus
                    .handle<LoadModel>([](LoadModel const& c) -> LoadModelReply {
                        return LoadModelReply{.ok = false, .handle = c.path};
                    })
                    .has_value());

    LoadModelReply r{};
    ASSERT_TRUE(bus.send<LoadModelReply>(LoadModel{.path = "p"}).has_value());
    auto reply = bus.send<LoadModelReply>(LoadModel{.path = "p"});
    ASSERT_TRUE(reply.has_value());
    r = *reply;
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.handle, "p");
}
