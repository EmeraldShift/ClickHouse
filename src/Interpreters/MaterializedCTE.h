#pragma once

#include <Common/EventFD.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage_fwd.h>
#include <base/defines.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class QueryPlan;

/// Owns a temporary Memory table used by a materialized CTE, and tracks whether
/// the table has already been populated. The `is_built` flag is checked
/// atomically in `MaterializingCTETransform` so that when the same CTE is
/// referenced from multiple places (e.g. two IN-subqueries or an IN-subquery
/// and the main plan), the table is written exactly once.
struct MaterializedCTE
{
    explicit MaterializedCTE(const std::string & cte_name_);

    MaterializedCTE(const MaterializedCTE &) = delete;
    MaterializedCTE & operator=(const MaterializedCTE &) = delete;

    ~MaterializedCTE() noexcept;

    bool isStorageInitialized() const noexcept
    {
        return storage != nullptr;
    }

    bool hasPlanOrBuilt() const noexcept
    {
        return plan != nullptr || is_built;
    }

    TemporaryTableHolder extractTableHolder()
    {
        chassert(table_holder.has_value());
        return std::move(*table_holder);
    }

    /// Temporary table storage.
    StoragePtr storage = {};
    /// Temporary table storage.
    std::optional<TemporaryTableHolder> table_holder = {};
    /// Name of the CTE.
    const std::string cte_name;
    /// Temporary table name
    const std::string temporary_table_name;
    /// Query Plan for the CTE
    std::unique_ptr<QueryPlan> plan = {};
    /// If true, query plan is built for the CTE (i.e. the table is being populated, but is not ready for reads yet).
    std::atomic_bool is_materialization_planned{false};
    /// If true, the CTE has been materialized (i.e. the table has been populated and is ready for reads).
    std::atomic_bool is_built{false};
};

using MaterializedCTEPtr = std::shared_ptr<MaterializedCTE>;


/** Build-once / wait-many coordination primitive for a materialized CTE.
  *
  * Models the CTE's populated storage as a future: the first consumer to need
  * it triggers the build, subsequent consumers wait on the same shared_future,
  * and correctness is a property of this object rather than of the query plan
  * topology. This mirrors the promise/shared_future pattern already used by
  * `PreparedSetsCache::findOrPromiseToBuild` for cross-task set construction
  * (see src/Interpreters/PreparedSets.cpp).
  *
  * The caller supplies a `Builder` at construction time which, given a query
  * context, runs the CTE's source pipeline to completion and returns the
  * populated `StoragePtr`. The builder is invoked exactly once across all
  * `buildInplace` / `getOrScheduleBuild` callers; concurrent invocations wait
  * on the same shared_future.
  *
  * State transitions are linear:
  *   NotStarted -> Building -> { Built, Failed }
  * Once Built or Failed, state is terminal.
  *
  * The class carries four synchronization primitives, each with a distinct
  * role — collapsing any of them would regress one of the consumer patterns:
  *
  *   std::atomic<State>:
  *     Lock-free fast path for `tryGet` / `isBuilt` readers, paired with
  *     a release-store from the builder so consumers observe the populated
  *     `storage` the moment they see state==Built, without acquiring the
  *     mutex.
  *
  *   std::mutex:
  *     Ensures exactly one caller wins the NotStarted -> Building
  *     transition, and guards the `storage` write from external-completion
  *     entry points racing with the internal builder.
  *
  *   std::promise + std::shared_future:
  *     Exception-propagating blocking wait for consumers that can't
  *     return Status::Async (non-Linux readers, EXPLAIN, anything reached
  *     before `MemorySource::prepare` fires).
  *
  *   EventFD (Linux only):
  *     Level-triggered readiness signal for Status::Async readers. Written
  *     once at the Built / Failed transition, never read; consumers dup(2)
  *     their own fd so all of them wake on the single write.
  */
class FutureMaterializedCTE : public std::enable_shared_from_this<FutureMaterializedCTE>
{
public:
    using Builder = std::function<StoragePtr(const ContextPtr &)>;

    /// A `Scheduler` dispatches a job asynchronously (e.g. onto a thread pool).
    /// `getOrScheduleBuild` uses this to kick off the build off the caller's
    /// thread; `buildInplace` never uses it.
    using Scheduler = std::function<void(std::function<void()>)>;

    enum class State : uint8_t
    {
        NotStarted,
        Building,
        Built,
        Failed,
    };

    /// `builder` may be null. When null, `buildInplace` and
    /// `getOrScheduleBuild` raise `LOGICAL_ERROR`. External agents (e.g. the
    /// materialization transform running in the main pipeline) can still
    /// drive completion through `markBuilt` / `markFailed`; the null-builder
    /// overload is intended for tests that drive the state machine directly.
    explicit FutureMaterializedCTE(std::string cte_name_, Builder builder_ = {});

    FutureMaterializedCTE(const FutureMaterializedCTE &) = delete;
    FutureMaterializedCTE & operator=(const FutureMaterializedCTE &) = delete;

    /// Synchronous build-or-wait.
    ///
    /// First caller transitions NotStarted -> Building and runs the builder on
    /// its own thread. Concurrent callers wait on the shared_future.
    /// Rethrows the builder's exception if the build failed.
    ///
    /// Used by eager consumers during optimization (e.g. `KeyCondition` via
    /// `buildOrderedSetInplace`, `DelayedCreatingSetsStep::makePlansForSets`,
    /// `evaluateScalarSubqueryIfNeeded`).
    StoragePtr buildInplace(const ContextPtr & context);

    /// Returns a shared_future that completes when the CTE is built.
    ///
    /// If no build has started, transitions NotStarted -> Building and
    /// dispatches the build onto `scheduler` before returning. If the build is
    /// already in flight or finished, just returns the existing future.
    ///
    /// Used by readers that need to barrier at first data pull but don't want
    /// to execute the build themselves.
    std::shared_future<StoragePtr> getOrScheduleBuild(
        const ContextPtr & context,
        const Scheduler & scheduler);

    /// Non-blocking fast-path: returns populated storage if the build is
    /// complete, else nullptr. Does not wait and does not throw.
    StoragePtr tryGet() const noexcept;

    bool isBuilt() const noexcept
    {
        return state.load(std::memory_order_acquire) == State::Built;
    }

    State getState() const noexcept
    {
        return state.load(std::memory_order_acquire);
    }

    const std::string & name() const noexcept { return cte_name; }

    /// Linux: returns an eventfd that becomes readable when the build reaches
    /// a terminal state (Built or Failed). Readers in a Processor pipeline
    /// can return `Status::Async` from `prepare()` and hand this fd to the
    /// executor's `schedule()`, avoiding the blocking `shared_future::get()`
    /// path and keeping worker threads reclaimable while the build runs.
    ///
    /// Non-Linux: returns -1. Callers must fall back to the blocking
    /// `buildInplace(ctx)` barrier.
    int getReadinessFd() const noexcept;

    /// External-completion APIs. Fulfill the shared_future from a pipeline
    /// running outside of buildInplace/getOrScheduleBuild (i.e. the legacy
    /// main-pipeline MaterializingCTETransform path).
    ///
    /// Both are idempotent: if the future has already left NotStarted
    /// (whether via the internal builder or an earlier external mark)
    /// the call is a silent no-op. Callers do not need to pre-check
    /// state.
    void markBuilt(StoragePtr storage_);
    void markFailed(std::exception_ptr ex);

private:
    /// Runs the builder on the current thread and fulfills the promise.
    /// Must be called exactly once, by the caller that observed NotStarted and
    /// flipped the state to Building. Not called while holding `mutex`.
    void runBuildAndFulfillPromise(const ContextPtr & context);

    /// Wake any reader poll'ing the readiness eventfd. No-op on non-Linux.
    /// Called once per FutureMaterializedCTE at the Built / Failed
    /// transition; safe to call from any of the three terminal-transition
    /// paths (runBuildAndFulfillPromise, markBuilt, markFailed).
    void signalReadiness() noexcept;

    const std::string cte_name;
    const Builder builder;

    mutable std::mutex mutex;
    std::atomic<State> state{State::NotStarted};

    /// Promise is fulfilled by the builder thread; `build_future` is a
    /// shared view that every waiter (including the builder itself, to avoid
    /// returning stale storage) reads through.
    std::promise<StoragePtr> build_promise;
    std::shared_future<StoragePtr> build_future;

    /// Populated by the builder thread before transitioning state to Built.
    /// Subsequent readers can read this directly via a release-acquire
    /// handshake on `state`.
    StoragePtr storage;

    /// Signalled once, when the build reaches Built or Failed. Empty struct
    /// on non-Linux platforms (getReadinessFd returns -1 there).
    EventFD readiness_event_fd;
};

using FutureMaterializedCTEPtr = std::shared_ptr<FutureMaterializedCTE>;

}
