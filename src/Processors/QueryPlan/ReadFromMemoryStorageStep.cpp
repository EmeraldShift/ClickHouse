#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>

#include <Analyzer/TableNode.h>

#include <Common/typeid_cast.h>

#include <Common/CurrentThread.h>
#include <Common/ErrnoException.h>

#include <base/defines.h>

#if defined(OS_LINUX)
#include <fcntl.h>
#include <unistd.h>
#endif

#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MaterializedCTE.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>
#include <Storages/VirtualColumnUtils.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>

#include <atomic>
#include <functional>
#include <memory>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;
extern const int CANNOT_FCNTL;

}

class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;

    static Block getHeader(const NamesAndTypesList & physical, const NamesAndTypesList & virtuals)
    {
        Block res;
        for (const auto & name_type : physical)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        for (const auto & name_type : virtuals)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        return res;
    }

public:
    MemorySource(
        NamesAndTypesList physical_columns_,
        NamesAndTypesList virtual_columns_,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {},
        MaterializedCTEPtr materialized_cte_ = {})
        : ISource(std::make_shared<const Block>(getHeader(physical_columns_, virtual_columns_)))
        , physical_columns(std::move(physical_columns_))
        , virtual_columns(std::move(virtual_columns_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
        , materialized_cte(std::move(materialized_cte_))
    {
    }

    String getName() const override { return "Memory"; }

#if defined(OS_LINUX)
    ~MemorySource() override
    {
        if (readiness_fd_dup >= 0)
            ::close(readiness_fd_dup);
    }
#endif

    /// If a materialized CTE is attached and its future hasn't reached a
    /// terminal state (Built / Failed), return Status::Async and expose the
    /// CTE's readiness eventfd via schedule(). The executor reclaims the
    /// worker, polls the fd, and re-invokes prepare() when the build
    /// completes. Keeps reader workers fully available for unrelated work
    /// instead of blocking on `shared_future::get()` inside generate().
    ///
    /// Non-Linux: skipped; generate()'s buildInplace barrier (inside the
    /// initializer_func path) remains the fallback wait mechanism.
    Status prepare() override
    {
        /// Run the base prepare first so that cancellation, downstream
        /// close (output finished), and any buffered-chunk state are
        /// resolved before we consider blocking on the CTE future. We
        /// only intercept when the base says "ready to generate" —
        /// any other status (Finished, PortFull, NeedData, Async) is
        /// the correct answer already.
        Status status = ISource::prepare();
        if (status != Status::Ready)
            return status;

#if defined(OS_LINUX)
        if (materialized_cte && materialized_cte->future)
        {
            const auto s = materialized_cte->future->getState();
            if (s == FutureMaterializedCTE::State::NotStarted)
            {
                /// Safety net: the owning
                /// `DelayedMaterializingCTEsStep::makePlansForCTEs`
                /// already dispatches the build via getOrScheduleBuild
                /// on the global pool, so state == NotStarted here is
                /// not expected. Dispatch defensively anyway so the
                /// reader does not deadlock if some future path forgets
                /// to — the fd will fire once the job lands.
                ///
                /// Capture the thread group so the CTE build runs with
                /// this query's MemoryTracker / cancellation flag /
                /// ProfileEvents, not the pool worker's.
                materialized_cte->future->getOrScheduleBuild(
                    CurrentThread::tryGetQueryContext(),
                    makeMaterializeCTEScheduler());
                return Status::Async;
            }
            if (s == FutureMaterializedCTE::State::Building)
                return Status::Async;
            /// Built or Failed: fall through. generate()'s buildInplace
            /// call is a fast no-op on Built; on Failed it rethrows the
            /// builder's exception via shared_future::get(), surfacing
            /// the error to the pipeline correctly.
        }
#endif

        return status;
    }

    int schedule() override
    {
#if defined(OS_LINUX)
        /// Multiple MemorySources read from the same MaterializedCTE and
        /// therefore from the same FutureMaterializedCTE, so handing back
        /// the raw readiness fd would make the pipeline executor try to
        /// `epoll_ctl(ADD)` one fd multiple times — second call errors
        /// with EEXIST. Give each source its own `dup(2)` of the
        /// underlying eventfd: distinct fd numbers (so epoll is happy),
        /// shared underlying state (so they all wake when the build
        /// signals). We never `read()` the counter, so all dups stay
        /// readable after the first signal; that's fine because each
        /// processor's prepare() falls through to the normal ISource
        /// path once it observes Built / Failed.
        if (readiness_fd_dup < 0 && materialized_cte && materialized_cte->future)
        {
            int src = materialized_cte->future->getReadinessFd();
            if (src >= 0)
            {
                readiness_fd_dup = ::dup(src);
                if (readiness_fd_dup < 0)
                    throw ErrnoException(
                        ErrorCodes::CANNOT_FCNTL,
                        "Cannot dup materialized-CTE readiness fd for pipeline source");
                if (::fcntl(readiness_fd_dup, F_SETFD, FD_CLOEXEC) < 0)
                    throw ErrnoException(
                        ErrorCodes::CANNOT_FCNTL,
                        "Cannot set FD_CLOEXEC on duped materialized-CTE readiness fd");
            }
        }
        return readiness_fd_dup;
#else
        return -1;
#endif
    }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            if (materialized_cte)
            {
                /// Barrier on the CTE's materialization future. The
                /// first reader to hit this call site either finds the
                /// build already complete (fast path: atomic load, no
                /// wait), waits on an in-flight build, or — if nothing
                /// has driven the build yet — kicks it off synchronously
                /// via the installed builder. If the build failed, the
                /// builder's exception propagates through
                /// `shared_future::get()` and surfaces here.
                ///
                /// This replaces the pre-refactor "throw if not built"
                /// gate: correctness is now a property of the future
                /// barrier rather than of plan-tree placement. Main-
                /// pipeline readers that race ahead of an async-
                /// scheduled CTE build simply wait here; under the
                /// synchronous `makePlansForCTEs` path, the barrier is a
                /// no-op because the build has already completed during
                /// plan optimization.
                ///
                /// `MaterializedCTE::create` is the only construction
                /// path and it always installs a future, so the handle
                /// is non-null by invariant; the chassert catches any
                /// violation in debug builds.
                chassert(materialized_cte->future);
                if (!materialized_cte->future->isBuilt())
                    materialized_cte->future->buildInplace(CurrentThread::tryGetQueryContext());
            }

            initializer_func(data);
            initializer_func = {};
        }

        Columns columns;
        columns.reserve(physical_columns.size() + virtual_columns.size());
        fillPhysicalColumns(columns);

        UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
        if (!columns.empty())
            fillVirtualColumns(columns, num_rows);

        return Chunk(std::move(columns), num_rows);
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }

        return execution_index++;
    }

    void fillPhysicalColumns(Columns & result_columns)
    {
        size_t current_index = getAndIncrementExecutionIndex();

        if (!data || current_index >= data->size())
            return;

        const Block & src = (*data)[current_index];

        for (const auto & name_and_type : physical_columns)
        {
            if (name_and_type.isSubcolumn())
                result_columns.emplace_back(tryGetSubcolumnFromBlock(src, name_and_type.getTypeInStorage(), name_and_type));
            else
                result_columns.emplace_back(tryGetColumnFromBlock(src, name_and_type));
        }

        fillMissingColumns(result_columns, src.rows(), physical_columns, physical_columns, {}, nullptr);
        assert(std::all_of(result_columns.begin(), result_columns.end(), [](const auto & column) { return column != nullptr; }));
    }

    void fillVirtualColumns([[maybe_unused]] Columns & result_columns, [[maybe_unused]] UInt64 num_rows) const
    {
        if (!virtual_columns.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual columns: '{}'", virtual_columns.getNames());
    }

    const NamesAndTypesList physical_columns;
    const NamesAndTypesList virtual_columns;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
    MaterializedCTEPtr materialized_cte;
#if defined(OS_LINUX)
    /// Per-source `dup(2)` of the CTE future's readiness eventfd, lazily
    /// initialized in schedule(). -1 until first schedule() call. Closed
    /// in the destructor.
    int readiness_fd_dup = -1;
#endif
};

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    const size_t num_streams_,
    const bool delay_read_for_global_sub_queries_)
    : SourceStepWithFilter(
        std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns_to_read_)),
        columns_to_read_,
        query_info_,
        storage_snapshot_,
        context_)
    , columns_to_read(columns_to_read_)
    , storage(std::move(storage_))
    , num_streams(num_streams_)
    , delay_read_for_global_sub_queries(delay_read_for_global_sub_queries_)
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    pipeline.init(std::move(pipe));
}

QueryPlanStepPtr ReadFromMemoryStorageStep::clone() const
{
    return std::make_unique<ReadFromMemoryStorageStep>(*this);
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

    auto [physical_column_names, virtual_column_names] = VirtualColumnUtils::splitPhysicalAndVirtualColumnNames(columns_to_read, storage_snapshot);
    auto physical_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), physical_column_names);
    auto virtual_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader), virtual_column_names);

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_sub_queries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            physical_columns,
            virtual_columns,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [my_storage = storage](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = assert_cast<const StorageMemory &>(*my_storage).data.get();
            },
            typeid_cast<StorageMemory *>(storage.get())->getMaterializedCTE()));
    }

    size_t size = current_data->size();
    num_streams = std::min(num_streams, size);
    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto source = std::make_shared<MemorySource>(physical_columns, virtual_columns, current_data, parallel_execution_index);
        if (stream == 0)
            source->addTotalRowsApprox(snapshot_data.rows_approx);
        pipes.emplace_back(std::move(source));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
