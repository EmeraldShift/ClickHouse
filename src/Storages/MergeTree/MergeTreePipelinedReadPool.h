#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

#include <atomic>
#include <deque>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

/// Setting-gated read pool that pipelines per-part index analysis with reading.
///
/// The pool starts from the parts/ranges that survived cheap global pruning.  A
/// worker that finds no ready read task claims one whole part, runs the normal
/// part-local PK/eager skip-index analysis, applies prepared read-time index
/// results, and publishes the surviving ranges as read tasks.  Ready read tasks
/// are always consumed before more producer work is claimed.
///
/// This intentionally does not split analysis within a part.  It is the first
/// production-oriented step: remove the whole-query index-analysis barrier while
/// keeping the existing per-part index semantics.
class MergeTreePipelinedReadPool final : public MergeTreeReadPoolBase
{
public:
    struct AnalysisContext
    {
        StorageMetadataPtr metadata_snapshot;
        bool is_final_query = false;
        ContextPtr context;
        std::shared_ptr<const ReadFromMergeTree::Indexes> indexes;
        std::optional<TopKFilterInfo> top_k_filter_info;
        LoggerPtr log;
        size_t num_streams = 1;
        bool find_exact_ranges = false;
        bool is_parallel_reading_from_replicas = false;
        bool has_projections = false;
        MergeTreeIndexBuildContextPtr index_build_context;
        std::optional<size_t> query_condition_cache_hash;
        String query_condition_cache_text;
    };

    MergeTreePipelinedReadPool(
        RangesInDataParts && parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const IndexReadTasks & index_read_tasks_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_,
        const AnalysisContext & analysis_context_);

    String getName() const override { return "PipelinedReadPool"; }
    bool preservesOrderOfRanges() const override { return false; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
    void cancel() noexcept override;

private:
    struct ReadyTask
    {
        size_t part_idx = 0;
        MarkRanges ranges;
    };

    struct PendingTopKPart
    {
        RangesInDataPart part;
        bool has_top_k_granules = false;
        std::vector<MergeTreeIndexBulkGranulesMinMax::MinMaxGranule> top_k_granules;
    };

    bool useTopKBarrier() const;
    void enqueueTopKReadyPartsIfComplete(size_t part_idx, RangesInDataPart part, MergeTreeDataSelectExecutor::PerPartIndexAnalysisStats stats);
    void analyzePartAndEnqueueTasks(size_t part_idx);
    void enqueueAnalyzedRanges(size_t part_idx, MarkRanges ranges);
    void writeQueryConditionCacheForSkippedRanges(size_t part_idx, const MarkRanges & kept_ranges) const;
    MarkRanges applyPreparedIndexResult(size_t part_idx, const RangesInDataPart & analyzed_part, MarkRanges ranges);

    mutable std::mutex mutex;
    std::deque<size_t> pending_parts TSA_GUARDED_BY(mutex);
    std::deque<ReadyTask> ready_tasks TSA_GUARDED_BY(mutex);
    std::vector<std::optional<PendingTopKPart>> pending_top_k_parts TSA_GUARDED_BY(mutex);
    size_t pending_top_k_parts_remaining TSA_GUARDED_BY(mutex) = 0;
    mutable std::mutex qcc_write_mutex;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    AnalysisContext analysis_context;
    std::atomic_bool is_cancelled = false;
};

}
