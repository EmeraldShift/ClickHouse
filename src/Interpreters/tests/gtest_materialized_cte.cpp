#include <Interpreters/MaterializedCTE.h>

#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Scheduler that runs jobs on std::threads owned by the scheduler itself.
/// Threads are joined in the scheduler's destructor, so any captures in
/// the job (weak pointers into the test's FutureMaterializedCTE, etc.)
/// cannot outlive the test fixture.
struct ThreadScheduler
{
    ~ThreadScheduler()
    {
        for (auto & t : threads)
            if (t.joinable())
                t.join();
    }

    void operator()(std::function<void()> job)
    {
        std::lock_guard lock(mutex);
        threads.emplace_back(std::move(job));
    }

    std::mutex mutex;
    std::vector<std::thread> threads;
};

}

TEST(FutureMaterializedCTE, BuildInplaceRunsBuilderExactlyOnce)
{
    std::atomic<int> build_count{0};

    FutureMaterializedCTE fut("cte", [&](const ContextPtr &) -> StoragePtr {
        ++build_count;
        return nullptr;
    });

    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::NotStarted);
    EXPECT_FALSE(fut.isBuilt());
    EXPECT_EQ(fut.tryGet(), nullptr);

    auto storage = fut.buildInplace(nullptr);
    EXPECT_EQ(build_count.load(), 1);
    EXPECT_TRUE(fut.isBuilt());
    EXPECT_EQ(storage, nullptr); /// builder returned nullptr, that's what we get back

    /// Second call is a no-op that returns the same storage.
    auto storage2 = fut.buildInplace(nullptr);
    EXPECT_EQ(build_count.load(), 1);
    EXPECT_EQ(storage2, storage);
}

TEST(FutureMaterializedCTE, ConcurrentBuildInplaceCallersSerializeThroughOneBuild)
{
    constexpr int num_threads = 16;

    std::atomic<int> build_count{0};
    std::atomic<int> concurrent_builders{0};
    std::atomic<int> max_concurrent_builders{0};

    /// Gate the builder so many threads are guaranteed to arrive while one is
    /// actively running.
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool release_gate = false;

    FutureMaterializedCTE fut("cte", [&](const ContextPtr &) -> StoragePtr {
        int now = ++concurrent_builders;
        int prev_max = max_concurrent_builders.load();
        while (now > prev_max && !max_concurrent_builders.compare_exchange_weak(prev_max, now)) {}

        ++build_count;

        {
            std::unique_lock lock(gate_mutex);
            gate_cv.wait(lock, [&] { return release_gate; });
        }

        --concurrent_builders;
        return nullptr;
    });

    std::vector<std::thread> threads;
    std::atomic<int> arrived{0};
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&] {
            ++arrived;
            fut.buildInplace(nullptr);
        });
    }

    /// Wait for all threads to have reached buildInplace before releasing the
    /// builder — maximizes contention on the NotStarted->Building transition.
    while (arrived.load() < num_threads)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    {
        std::lock_guard lock(gate_mutex);
        release_gate = true;
    }
    gate_cv.notify_all();

    for (auto & t : threads)
        t.join();

    EXPECT_EQ(build_count.load(), 1);
    EXPECT_EQ(max_concurrent_builders.load(), 1);
    EXPECT_TRUE(fut.isBuilt());
}

TEST(FutureMaterializedCTE, GetOrScheduleBuildDispatchesOnceAndWaitersSeeResult)
{
    constexpr int num_waiters = 8;

    std::atomic<int> build_count{0};

    /// Needs make_shared because the async dispatch path relies on
    /// weak_from_this() to avoid use-after-free on queued jobs.
    auto fut = std::make_shared<FutureMaterializedCTE>("cte",
        [&](const ContextPtr &) -> StoragePtr {
            ++build_count;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return nullptr;
        });

    ThreadScheduler scheduler;

    std::vector<std::shared_future<StoragePtr>> futures;
    futures.reserve(num_waiters);
    for (int i = 0; i < num_waiters; ++i)
        futures.push_back(fut->getOrScheduleBuild(nullptr, std::ref(scheduler)));

    for (auto & f : futures)
        EXPECT_EQ(f.get(), nullptr);

    EXPECT_EQ(build_count.load(), 1);
    EXPECT_TRUE(fut->isBuilt());
}

TEST(FutureMaterializedCTE, BuilderExceptionPropagatesToAllWaiters)
{
    FutureMaterializedCTE fut("cte", [](const ContextPtr &) -> StoragePtr {
        throw std::runtime_error("boom");
    });

    /// Spawn a handful of waiters blocked on getOrScheduleBuild, plus the
    /// initial buildInplace that triggers the (eventual) failure.
    std::vector<std::thread> threads;
    std::atomic<int> exceptions_seen{0};

    for (int i = 0; i < 4; ++i)
    {
        threads.emplace_back([&] {
            try
            {
                fut.buildInplace(nullptr);
            }
            catch (const std::runtime_error &)
            {
                ++exceptions_seen;
            }
        });
    }

    for (auto & t : threads)
        t.join();

    EXPECT_EQ(exceptions_seen.load(), 4);
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::Failed);
    EXPECT_FALSE(fut.isBuilt());
    EXPECT_EQ(fut.tryGet(), nullptr);

    /// Subsequent callers still see the same failure.
    EXPECT_THROW(fut.buildInplace(nullptr), std::runtime_error);
}

TEST(FutureMaterializedCTE, TryGetFastPathAfterBuilt)
{
    FutureMaterializedCTE fut("cte", [](const ContextPtr &) -> StoragePtr {
        return nullptr;
    });

    EXPECT_EQ(fut.tryGet(), nullptr); /// NotStarted; not a value we can read
    EXPECT_FALSE(fut.isBuilt());

    fut.buildInplace(nullptr);

    EXPECT_TRUE(fut.isBuilt());
    EXPECT_EQ(fut.tryGet(), nullptr); /// Built + storage is nullptr — still safe, never blocks
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::Built);
}

TEST(FutureMaterializedCTE, BuildInplaceRejectsNullBuilder)
{
    FutureMaterializedCTE fut("cte"); /// no builder
    EXPECT_THROW(fut.buildInplace(nullptr), DB::Exception);
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::NotStarted);
}

TEST(FutureMaterializedCTE, GetOrScheduleBuildRejectsNullBuilder)
{
    FutureMaterializedCTE fut("cte"); /// no builder
    ThreadScheduler scheduler;
    EXPECT_THROW(
        fut.getOrScheduleBuild(nullptr, std::ref(scheduler)),
        DB::Exception);
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::NotStarted);
}

TEST(FutureMaterializedCTE, GetOrScheduleBuildRejectsNullScheduler)
{
    FutureMaterializedCTE fut("cte", [](const ContextPtr &) -> StoragePtr {
        return nullptr;
    });
    EXPECT_THROW(
        fut.getOrScheduleBuild(nullptr, FutureMaterializedCTE::Scheduler{}),
        DB::Exception);
}

TEST(FutureMaterializedCTE, MarkBuiltFulfillsFutureAndWakesWaiters)
{
    FutureMaterializedCTE fut("cte"); /// no builder — external completion only

    /// Collect a bunch of waiters on the shared_future before anyone marks built.
    std::vector<std::thread> waiters;
    std::atomic<int> completed{0};
    /// getOrScheduleBuild requires a builder, so we can't use it here.
    /// Instead waiters use a raw reference to the shared_future that is
    /// obtained through a buildInplace path — but buildInplace also needs a
    /// builder. For external-completion tests we have to expose the future
    /// via markBuilt's wakeup effect alone, which we observe by polling
    /// tryGet.

    for (int i = 0; i < 4; ++i)
    {
        waiters.emplace_back([&] {
            while (!fut.isBuilt())
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            ++completed;
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    fut.markBuilt(nullptr);

    for (auto & t : waiters)
        t.join();

    EXPECT_EQ(completed.load(), 4);
    EXPECT_TRUE(fut.isBuilt());
    EXPECT_EQ(fut.tryGet(), nullptr); /// markBuilt with null storage is intentional in this test
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::Built);
}

TEST(FutureMaterializedCTE, MarkFailedIsTerminalAndLaterMarksAreNoOps)
{
    FutureMaterializedCTE fut("cte");

    auto ex = std::make_exception_ptr(std::runtime_error("external boom"));
    fut.markFailed(ex);

    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::Failed);
    EXPECT_FALSE(fut.isBuilt());
    EXPECT_EQ(fut.tryGet(), nullptr);

    /// Subsequent marks are silent no-ops — state stays Failed, does
    /// not transition to Built or re-arm the promise with a different
    /// exception.
    fut.markBuilt(nullptr);
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::Failed);
    fut.markFailed(std::make_exception_ptr(std::runtime_error("again")));
    EXPECT_EQ(fut.getState(), FutureMaterializedCTE::State::Failed);
}

TEST(FutureMaterializedCTE, BuildInplaceAndGetOrScheduleBuildCoexist)
{
    /// One thread uses buildInplace (becomes the builder), many threads use
    /// getOrScheduleBuild (become waiters); every caller sees the same result.
    std::atomic<int> build_count{0};

    auto fut = std::make_shared<FutureMaterializedCTE>("cte",
        [&](const ContextPtr &) -> StoragePtr {
            ++build_count;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            return nullptr;
        });

    ThreadScheduler scheduler;

    std::vector<std::shared_future<StoragePtr>> futures;
    for (int i = 0; i < 4; ++i)
        futures.push_back(fut->getOrScheduleBuild(nullptr, std::ref(scheduler)));

    std::thread inline_builder([&] { fut->buildInplace(nullptr); });

    for (auto & f : futures)
        EXPECT_EQ(f.get(), nullptr);
    inline_builder.join();

    EXPECT_EQ(build_count.load(), 1);
    EXPECT_TRUE(fut->isBuilt());
}
