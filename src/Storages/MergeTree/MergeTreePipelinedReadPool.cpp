#include <Storages/MergeTree/MergeTreePipelinedReadPool.h>

#include <Storages/MergeTree/MergeTreeIndexReadResultPool.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace
{

MarkRanges intersectMarkRangesLocal(const MarkRanges & a, const MarkRanges & b)
{
    MarkRanges result;
    const auto * it_a = a.begin();
    const auto * it_b = b.begin();

    while (it_a != a.end() && it_b != b.end())
    {
        const size_t begin = std::max(it_a->begin, it_b->begin);
        const size_t end = std::min(it_a->end, it_b->end);

        if (begin < end)
        {
            if (!result.empty() && result.back().end == begin)
                result.back().end = end;
            else
                result.emplace_back(begin, end);
        }

        if (it_a->end < it_b->end)
            ++it_a;
        else
            ++it_b;
    }

    return result;
}

MarkRanges markRangesFromSelectedGranules(const std::vector<bool> & selected, const MarkRanges & candidate)
{
    MarkRanges result;
    for (const auto & range : candidate)
    {
        size_t mark = range.begin;
        while (mark < range.end && mark < selected.size())
        {
            if (!selected[mark])
            {
                ++mark;
                continue;
            }

            const size_t begin = mark;
            while (mark < range.end && mark < selected.size() && selected[mark])
                ++mark;

            if (!result.empty() && result.back().end == begin)
                result.back().end = mark;
            else
                result.emplace_back(begin, mark);
        }
    }
    return result;
}

MarkRanges subtractMarkRangesLocal(const MarkRanges & full, const MarkRanges & kept)
{
    MarkRanges result;
    size_t kept_index = 0;

    for (const auto & full_range : full)
    {
        size_t cursor = full_range.begin;

        while (kept_index < kept.size() && kept[kept_index].end <= cursor)
            ++kept_index;

        size_t local_kept_index = kept_index;
        while (local_kept_index < kept.size() && kept[local_kept_index].begin < full_range.end)
        {
            const auto & kept_range = kept[local_kept_index];
            if (cursor < kept_range.begin)
                result.emplace_back(cursor, std::min(kept_range.begin, full_range.end));

            cursor = std::max(cursor, kept_range.end);
            if (cursor >= full_range.end)
                break;

            ++local_kept_index;
        }

        if (cursor < full_range.end)
            result.emplace_back(cursor, full_range.end);
    }

    return result;
}

MarkRanges takeMarks(MarkRanges & ranges, size_t need_marks)
{
    MarkRanges result;
    while (need_marks > 0 && !ranges.empty())
    {
        auto & range = ranges.front();
        const size_t marks_in_range = range.end - range.begin;
        const size_t marks_to_take = std::min(marks_in_range, need_marks);
        result.emplace_back(range.begin, range.begin + marks_to_take);
        range.begin += marks_to_take;
        if (range.begin == range.end)
            ranges.pop_front();
        need_marks -= marks_to_take;
    }
    return result;
}

MarkRanges markRangesFromProjectionBitmap(
    const ProjectionIndexBitmap & bitmap,
    const MergeTreeIndexGranularity & granularity,
    const MarkRanges & candidate)
{
    MarkRanges result;
    for (const auto & range : candidate)
    {
        for (size_t mark = range.begin; mark < range.end; ++mark)
        {
            const size_t begin = granularity.getMarkStartingRow(mark);
            const size_t end = begin + granularity.getMarkRows(mark);
            if (bitmap.rangeAllZero(begin, end))
                continue;

            if (!result.empty() && result.back().end == mark)
                result.back().end = mark + 1;
            else
                result.emplace_back(mark, mark + 1);
        }
    }
    return result;
}

}

MergeTreePipelinedReadPool::MergeTreePipelinedReadPool(
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
    const AnalysisContext & analysis_context_)
    : MergeTreeReadPoolBase(
          std::move(parts_),
          std::move(mutations_snapshot_),
          std::move(shared_virtual_fields_),
          index_read_tasks_,
          storage_snapshot_,
          row_level_filter_,
          prewhere_info_,
          actions_settings_,
          reader_settings_,
          column_names_,
          settings_,
          params_,
          context_)
    , updater(std::move(updater_))
    , analysis_context(analysis_context_)
{
    for (size_t i = 0; i < per_part_infos.size(); ++i)
        pending_parts.push_back(i);

    if (useTopKBarrier())
    {
        pending_top_k_parts.resize(per_part_infos.size());
        pending_top_k_parts_remaining = per_part_infos.size();
    }
}

void MergeTreePipelinedReadPool::enqueueAnalyzedRanges(size_t part_idx, MarkRanges ranges)
{
    if (ranges.empty() || is_cancelled.load(std::memory_order_relaxed))
        return;

    std::lock_guard lock(mutex);
    if (is_cancelled.load(std::memory_order_relaxed))
        return;

    ready_tasks.push_back(ReadyTask{part_idx, std::move(ranges)});
}

void MergeTreePipelinedReadPool::writeQueryConditionCacheForSkippedRanges(size_t part_idx, const MarkRanges & kept_ranges) const
{
    if (!analysis_context.query_condition_cache_hash)
        return;

    const auto & full_ranges = parts_ranges[part_idx];
    MarkRanges skipped_ranges = subtractMarkRangesLocal(full_ranges.ranges, kept_ranges);
    if (skipped_ranges.empty())
        return;

    const auto & data_part = full_ranges.data_part;
    const String part_name = data_part->isProjectionPart()
        ? fmt::format("{}:{}", data_part->getParentPartName(), data_part->name)
        : data_part->name;

    auto query_condition_cache = Context::getGlobalContextInstance()->getQueryConditionCache();
    if (!query_condition_cache)
        return;

    /// The pipelined path can have several producer threads discover skipped
    /// ranges concurrently, so serialize the writes conservatively.
    std::lock_guard lock(qcc_write_mutex);
    query_condition_cache->write(
        data_part->storage.getStorageID().uuid,
        part_name,
        *analysis_context.query_condition_cache_hash,
        analysis_context.query_condition_cache_text,
        skipped_ranges,
        data_part->index_granularity->getMarksCount(),
        data_part->index_granularity->hasFinalMark());
}

MarkRanges MergeTreePipelinedReadPool::applyPreparedIndexResult(size_t part_idx, const RangesInDataPart & analyzed_part, MarkRanges ranges)
{
    if (!analysis_context.index_build_context || ranges.empty() || is_cancelled.load(std::memory_order_relaxed))
        return ranges;

    auto projection_it = analysis_context.index_build_context->projection_read_ranges.find(analyzed_part.part_index_in_query);
    static const RangesInDataParts empty_projection_ranges;
    const auto & projection_ranges = projection_it != analysis_context.index_build_context->projection_read_ranges.end()
        ? projection_it->second
        : empty_projection_ranges;

    const auto & all_updated_columns = per_part_infos[part_idx]->alter_conversions->getAllUpdatedColumns();
    auto part_for_prepared_result = analyzed_part;
    part_for_prepared_result.ranges = ranges;
    auto prepared = analysis_context.index_build_context->index_reader_pool->getOrBuildIndexReadResult(
        part_for_prepared_result,
        projection_ranges,
        analysis_context.metadata_snapshot,
        all_updated_columns);

    if (!prepared || is_cancelled.load(std::memory_order_relaxed))
        return ranges;

    if (prepared->skip_index_read_result)
    {
        const auto skip_ranges = markRangesFromSelectedGranules(
            prepared->skip_index_read_result->granules_selected,
            ranges);
        ranges = intersectMarkRangesLocal(ranges, skip_ranges);
    }

    if (prepared->projection_index_read_result && !ranges.empty())
    {
        const auto projection_ranges_for_part = markRangesFromProjectionBitmap(
            *prepared->projection_index_read_result,
            *per_part_infos[part_idx]->data_part->index_granularity,
            ranges);
        ranges = intersectMarkRangesLocal(ranges, projection_ranges_for_part);
    }

    /// The result was consumed by the producer.  Readers in this mode usually do
    /// not attach the read-time index context, so clear the compatibility registry
    /// now.  Dynamic TopK filtering is the exception: readers keep the context so
    /// `canSkipMark` can consult the live threshold tracker.
    if (!analysis_context.top_k_filter_info)
        analysis_context.index_build_context->index_reader_pool->clear(analyzed_part.data_part);

    return ranges;
}


bool MergeTreePipelinedReadPool::useTopKBarrier() const
{
    return analysis_context.top_k_filter_info
        && analysis_context.indexes->skip_indexes.skip_index_for_top_k_filtering
        && !analysis_context.top_k_filter_info->where_clause;
}

void MergeTreePipelinedReadPool::enqueueTopKReadyPartsIfComplete(
    size_t part_idx,
    RangesInDataPart part,
    MergeTreeDataSelectExecutor::PerPartIndexAnalysisStats stats)
{
    std::vector<std::pair<size_t, RangesInDataPart>> parts_to_publish;

    {
        std::lock_guard lock(mutex);
        if (is_cancelled.load(std::memory_order_relaxed))
            return;

        pending_top_k_parts[part_idx] = PendingTopKPart{
            .part = std::move(part),
            .has_top_k_granules = !stats.top_k_granules.empty(),
            .top_k_granules = std::move(stats.top_k_granules),
        };

        chassert(pending_top_k_parts_remaining > 0);
        --pending_top_k_parts_remaining;
        if (pending_top_k_parts_remaining != 0)
            return;

        chassert(analysis_context.top_k_filter_info);
        const auto & top_k_filter_info = *analysis_context.top_k_filter_info;
        const auto & top_k_index = analysis_context.indexes->skip_indexes.skip_index_for_top_k_filtering;
        chassert(top_k_index);

        std::vector<std::vector<MergeTreeIndexBulkGranulesMinMax::MinMaxGranule>> parts_top_k_granules(
            pending_top_k_parts.size());
        for (size_t i = 0; i < pending_top_k_parts.size(); ++i)
        {
            if (pending_top_k_parts[i] && pending_top_k_parts[i]->has_top_k_granules)
                parts_top_k_granules[i] = pending_top_k_parts[i]->top_k_granules;
        }

        const bool top_k_handle_ties = top_k_filter_info.num_sort_columns > 1 || top_k_index->index.granularity > 1;
        std::vector<MarkRanges> top_k_ranges;
        MergeTreeIndexBulkGranulesMinMax::getTopKMarks(
            top_k_filter_info.direction,
            top_k_filter_info.limit_n,
            top_k_index->index.granularity,
            top_k_handle_ties,
            parts_top_k_granules,
            top_k_ranges);

        parts_to_publish.reserve(pending_top_k_parts.size());
        for (size_t i = 0; i < pending_top_k_parts.size(); ++i)
        {
            if (!pending_top_k_parts[i])
                continue;

            auto ready_part = std::move(pending_top_k_parts[i]->part);
            if (pending_top_k_parts[i]->has_top_k_granules)
                ready_part.ranges = std::move(top_k_ranges[i]);

            if (!ready_part.ranges.empty())
                parts_to_publish.emplace_back(i, std::move(ready_part));
        }
    }

    for (auto & [ready_part_idx, ready_part] : parts_to_publish)
    {
        auto candidate_ranges = ready_part.ranges;
        ready_part.ranges = applyPreparedIndexResult(ready_part_idx, ready_part, std::move(candidate_ranges));
        writeQueryConditionCacheForSkippedRanges(ready_part_idx, ready_part.ranges);
        enqueueAnalyzedRanges(ready_part_idx, std::move(ready_part.ranges));
    }
}

void MergeTreePipelinedReadPool::analyzePartAndEnqueueTasks(size_t part_idx)
{
    if (is_cancelled.load(std::memory_order_relaxed))
        return;

    auto analyzed_part = parts_ranges[part_idx];

    ReadFromMergeTree::AnalysisResult scratch_result;
    MergeTreeDataSelectExecutor::IndexAnalysisContext part_analysis_context{
        .metadata_snapshot = analysis_context.metadata_snapshot,
        .mutations_snapshot = mutations_snapshot,
        .query_info = nullptr,
        .is_final_query = analysis_context.is_final_query,
        .context = analysis_context.context,
        .indexes = *analysis_context.indexes,
        .top_k_filter_info = analysis_context.top_k_filter_info,
        .reader_settings = reader_settings,
        .log = analysis_context.log,
        .num_streams = 1,
        .find_exact_ranges = analysis_context.find_exact_ranges,
        .is_parallel_reading_from_replicas = analysis_context.is_parallel_reading_from_replicas,
        .has_projections = analysis_context.has_projections,
        .result = scratch_result,
    };

    const auto & per_part_index_orders = analysis_context.indexes->skip_indexes.per_part_index_orders;
    const std::vector<size_t> * part_index_order = nullptr;
    if (!per_part_index_orders.empty())
    {
        const size_t original_part_index = analyzed_part.part_index_in_query;
        if (original_part_index < per_part_index_orders.size())
            part_index_order = &per_part_index_orders[original_part_index];
    }

    MergeTreeDataSelectExecutor::PerPartIndexAnalysisStats part_stats;
    auto analyzed = MergeTreeDataSelectExecutor::analyzePartByPrimaryKeyAndSkipIndexes(
        part_analysis_context,
        std::move(analyzed_part),
        part_index_order,
        scratch_result.index_stats,
        useTopKBarrier() ? &part_stats : nullptr);

    if (!analyzed || is_cancelled.load(std::memory_order_relaxed))
    {
        writeQueryConditionCacheForSkippedRanges(part_idx, {});
        return;
    }

    analyzed_part = std::move(*analyzed);
    if (useTopKBarrier())
    {
        enqueueTopKReadyPartsIfComplete(part_idx, std::move(analyzed_part), std::move(part_stats));
        return;
    }

    auto candidate_ranges = analyzed_part.ranges;
    analyzed_part.ranges = applyPreparedIndexResult(part_idx, analyzed_part, std::move(candidate_ranges));

    writeQueryConditionCacheForSkippedRanges(part_idx, analyzed_part.ranges);
    enqueueAnalyzedRanges(part_idx, std::move(analyzed_part.ranges));
}

MergeTreeReadTaskPtr MergeTreePipelinedReadPool::getTask(size_t, MergeTreeReadTask * previous_task)
{
    while (true)
    {
        if (is_cancelled.load(std::memory_order_relaxed))
            return nullptr;

        std::optional<ReadyTask> task_to_read;
        std::optional<size_t> part_to_analyze;

        {
            std::lock_guard lock(mutex);
            if (!ready_tasks.empty())
            {
                task_to_read = std::move(ready_tasks.front());
                ready_tasks.pop_front();
            }
            else if (!pending_parts.empty())
            {
                part_to_analyze = pending_parts.front();
                pending_parts.pop_front();
            }
            else
            {
                return nullptr;
            }
        }

        if (task_to_read)
        {
            size_t min_marks_per_task = std::max<size_t>(1, per_part_infos[task_to_read->part_idx]->min_marks_per_task);
            if (analysis_context.top_k_filter_info)
                min_marks_per_task = std::max<size_t>(min_marks_per_task, 4 * 1024 * 1024);

            MarkRanges task_ranges = takeMarks(task_to_read->ranges, min_marks_per_task);
            if (!task_to_read->ranges.empty() && !is_cancelled.load(std::memory_order_relaxed))
            {
                std::lock_guard lock(mutex);
                if (!is_cancelled.load(std::memory_order_relaxed))
                    ready_tasks.push_front(std::move(*task_to_read));
            }

            if (is_cancelled.load(std::memory_order_relaxed))
                return nullptr;

            return createTask(per_part_infos[task_to_read->part_idx], std::move(task_ranges), previous_task, updater);
        }

        analyzePartAndEnqueueTasks(*part_to_analyze);
    }
}

void MergeTreePipelinedReadPool::cancel() noexcept
{
    is_cancelled.store(true, std::memory_order_relaxed);

    if (analysis_context.index_build_context)
        analysis_context.index_build_context->index_reader_pool->cancel();

    std::lock_guard lock(mutex);
    pending_parts.clear();
    ready_tasks.clear();
}


}
