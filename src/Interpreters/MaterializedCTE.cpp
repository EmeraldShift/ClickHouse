#include <Interpreters/MaterializedCTE.h>

#include <Common/Exception.h>
#include <Common/thread_local_rng.h>
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Runs `cte`'s pre-built QueryPlan on a fresh CompletedPipelineExecutor in
/// the calling thread, then returns the populated storage. The
/// `MaterializingCTETransform` inside the pipeline is what actually writes
/// rows into `cte.storage` and (via its external-completion guard) leaves
/// the future's promise alone because the FutureMaterializedCTE is already
/// in State::Building — the caller (buildInplace) fulfills the promise with
/// the return value of this function.
///
StoragePtr runMaterializationPipeline(MaterializedCTE & cte, const ContextPtr & context)
{
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized CTE '{}' builder invoked without a context", cte.cte_name);
    if (!cte.plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized CTE '{}' has no plan to build", cte.cte_name);
    if (!cte.storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized CTE '{}' has no storage; finalizeMaterializedCTE must run first",
            cte.cte_name);

    QueryPlanOptimizationSettings optimization_settings(context);
    /// buildQueryPipeline runs optimize() internally when do_optimize=true
    /// (the default), so we don't call optimize() explicitly here.
    auto pipeline_builder = cte.plan->buildQueryPipeline(
        optimization_settings,
        BuildQueryPipelineSettings(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));
    pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    /// Tripwire: the pipeline completing implies `MaterializingCTETransform::generate()`
    /// ran and populated `cte.storage`. If a future refactor removes the transform from
    /// this pipeline, this assert fires instead of silently returning an empty CTE.
    chassert(cte.storage != nullptr);
    return cte.storage;
}

}

MaterializedCTE::MaterializedCTE(const std::string & cte_name_)
    : cte_name(cte_name_)
    , temporary_table_name(fmt::format("_materialized_cte_{}_{}", cte_name, thread_local_rng()))
{
    /// `future` is left null here and installed by `create()` after the
    /// shared_ptr exists, so the builder can capture a weak_ptr to the
    /// newly-constructed object. `create()` is the only caller that can
    /// reach this private constructor, so no one ever observes a
    /// MaterializedCTE with a null future in practice.
}

std::shared_ptr<MaterializedCTE> MaterializedCTE::create(const std::string & cte_name_)
{
    /// `new` rather than `std::make_shared` because the constructor is
    /// private; `make_shared`'s internal allocator can't reach it. One
    /// extra allocation per CTE, only at planning time.
    std::shared_ptr<MaterializedCTE> self(new MaterializedCTE(cte_name_));

    /// Weak-pointer capture: the builder may outlive the MaterializedCTE
    /// in pathological scenarios (e.g. if a FutureMaterializedCTEPtr gets
    /// stranded in a registry whose lifetime detaches from the query
    /// tree). `weak.lock()` in that case returns empty and we surface a
    /// clear error to the shared_future's waiters instead of use-after-
    /// free'ing into a destroyed object.
    std::weak_ptr<MaterializedCTE> weak_self = self;

    self->future = std::make_shared<FutureMaterializedCTE>(
        cte_name_,
        [weak_self](const ContextPtr & ctx) -> StoragePtr
        {
            auto locked = weak_self.lock();
            if (!locked)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "MaterializedCTE was destroyed before its materialization build could run");
            return runMaterializationPipeline(*locked, ctx);
        });

    return self;
}

MaterializedCTE::~MaterializedCTE() noexcept = default;

bool MaterializedCTE::hasPlanOrBuilt() const noexcept
{
    return plan != nullptr || (future && future->isBuilt());
}

FutureMaterializedCTE::FutureMaterializedCTE(std::string cte_name_, Builder builder_)
    : cte_name(std::move(cte_name_))
    , builder(std::move(builder_))
    , build_future(build_promise.get_future().share())
{
}

StoragePtr FutureMaterializedCTE::buildInplace(const ContextPtr & context)
{
    if (!builder)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "FutureMaterializedCTE::buildInplace called on '{}' without a builder", cte_name);

    bool must_build = false;
    std::shared_future<StoragePtr> future_copy;

    {
        std::lock_guard lock(mutex);

        const auto current_state = state.load(std::memory_order_relaxed);
        future_copy = build_future;

        if (current_state == State::NotStarted)
        {
            state.store(State::Building, std::memory_order_relaxed);
            must_build = true;
        }
    }

    if (must_build)
        runBuildAndFulfillPromise(context);

    /// Rethrows the builder's exception for every waiter if the build failed.
    return future_copy.get();
}

std::shared_future<StoragePtr> FutureMaterializedCTE::getOrScheduleBuild(
    const ContextPtr & context,
    const Scheduler & scheduler)
{
    if (!builder)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "FutureMaterializedCTE::getOrScheduleBuild called on '{}' without a builder", cte_name);
    if (!scheduler)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "FutureMaterializedCTE::getOrScheduleBuild called with null scheduler");

    bool must_schedule = false;
    std::shared_future<StoragePtr> future_copy;

    {
        std::lock_guard lock(mutex);

        const auto current_state = state.load(std::memory_order_relaxed);
        future_copy = build_future;

        if (current_state == State::NotStarted)
        {
            state.store(State::Building, std::memory_order_relaxed);
            must_schedule = true;
        }
    }

    if (must_schedule)
    {
        /// Capture a weak_ptr rather than `this`. The scheduled job may be
        /// queued for arbitrarily long; if the query is cancelled and every
        /// owner of this future drops before the pool worker dequeues it,
        /// the lock fails and the build silently no-ops. Any waiter on the
        /// shared_future has been torn down along with its query, so
        /// leaving the promise unfulfilled is correct.
        ///
        /// Enforce that the caller actually owns this through shared_ptr
        /// (otherwise weak_from_this() returns empty, every pool worker
        /// no-ops, and every reader barriers forever on an unfulfilled
        /// promise). Use `MaterializedCTE::create` in production, or
        /// `std::make_shared<FutureMaterializedCTE>(...)` in tests.
        std::weak_ptr<FutureMaterializedCTE> weak_self = weak_from_this();
        if (weak_self.expired())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "FutureMaterializedCTE::getOrScheduleBuild called on a non-shared instance; "
                "construct via std::make_shared so weak_from_this can track the owner");

        auto ctx_copy = context;
        scheduler([weak_self, ctx_copy]()
        {
            if (auto self = weak_self.lock())
                self->runBuildAndFulfillPromise(ctx_copy);
        });
    }

    return future_copy;
}

StoragePtr FutureMaterializedCTE::tryGet() const noexcept
{
    /// Release-acquire handshake with runBuildAndFulfillPromise: if we
    /// observe Built, the builder's write to `storage` happens-before this
    /// load, so it is safe to read `storage` without locking.
    if (state.load(std::memory_order_acquire) != State::Built)
        return nullptr;
    return storage;
}

void FutureMaterializedCTE::markBuilt(StoragePtr storage_)
{
    {
        std::lock_guard lock(mutex);
        if (state.load(std::memory_order_relaxed) != State::NotStarted)
            return;

        storage = storage_;
        /// Release store pairs with acquire loads in isBuilt/tryGet/getState.
        state.store(State::Built, std::memory_order_release);
    }
    build_promise.set_value(std::move(storage_));
    signalReadiness();
}

void FutureMaterializedCTE::markFailed(std::exception_ptr ex)
{
    if (!ex)
        ex = std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR,
            "FutureMaterializedCTE::markFailed called with null exception_ptr"));

    {
        std::lock_guard lock(mutex);
        if (state.load(std::memory_order_relaxed) != State::NotStarted)
            return;

        state.store(State::Failed, std::memory_order_release);
    }
    build_promise.set_exception(std::move(ex));
    signalReadiness();
}

void FutureMaterializedCTE::runBuildAndFulfillPromise(const ContextPtr & context)
{
    try
    {
        auto built = builder(context);

        storage = built;
        /// Release store: pairs with acquire loads in isBuilt / tryGet /
        /// getState so readers see the populated `storage` once they observe
        /// state == Built.
        state.store(State::Built, std::memory_order_release);
        build_promise.set_value(std::move(built));
    }
    catch (...)
    {
        state.store(State::Failed, std::memory_order_release);
        build_promise.set_exception(std::current_exception());
    }
    signalReadiness();
}

void FutureMaterializedCTE::signalReadiness() noexcept
{
#if defined(OS_LINUX)
    /// Wakes any reader polling the eventfd via Status::Async. Safe to call
    /// under all terminal transitions; the fd is level-triggered and edge-
    /// equivalent here because each FutureMaterializedCTE signals at most
    /// once (Built and Failed are terminal, and only one transition can
    /// reach this function). If the fd write fails we swallow the error
    /// because by this point the promise is fulfilled and any blocking
    /// waiter on shared_future::get() will unblock regardless.
    ///
    /// Nothing ever calls `read()` on the eventfd: each reader receives its
    /// own `dup`'d fd (see `MemorySource::schedule`) and every reader's fd
    /// needs to stay signaled so `epoll_wait` returns immediately once the
    /// build is resolved. Because the fd is level-triggered, leaving the
    /// value at 1 keeps all current and future dups primed.
    [[maybe_unused]] bool ok = readiness_event_fd.write();
#endif
}

int FutureMaterializedCTE::getReadinessFd() const noexcept
{
#if defined(OS_LINUX)
    return readiness_event_fd.fd;
#else
    return -1;
#endif
}

}
