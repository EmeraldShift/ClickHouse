#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Common/quoteString.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/StorageMergeTree.h>

#include <fmt/ranges.h>

#include <chrono>
#include <map>
#include <set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DELTA_QUERY_HISTORY_LOST;
    extern const int NOT_IMPLEMENTED;
}

using namespace DeltaQueryInternal;

namespace
{

/// The key of the time of the snapshot in the cursors of queries that call now(). Its value is in seconds.
constexpr auto time_key = "now";

CursorTreeNodePtr makeCursorTree(const Cursor & cursor)
{
    auto tree = std::make_shared<CursorTreeNode>();
    for (const auto & [key, partitions] : cursor)
        for (const auto & [partition_id, block_number] : partitions)
            tree->getSubtreeOrCreate(key)->getSubtreeOrCreate(partition_id)->setValue("block_number", block_number);
    return tree;
}

}

namespace DeltaQueryInternal
{

Int64 getBound(const std::map<String, Int64> & partitions, const String & partition_id)
{
    auto it = partitions.find(partition_id);
    return it == partitions.end() ? 0 : it->second;
}

std::map<String, TableBlocks> snapshotTables(const std::vector<Input> & inputs)
{
    std::map<String, TableBlocks> tables;
    for (const auto & input : inputs)
        tables.try_emplace(input.key, *input.storage);
    return tables;
}

std::optional<Int64> getTime(const CursorTreeNodePtr & tree)
{
    if (!tree || !tree->hasValue(time_key))
        return {};
    return tree->getValue(time_key);
}

Cursor parseCursor(const CursorTreeNodePtr & tree, const std::vector<Input> & inputs, const std::map<String, TableBlocks> & tables,
    std::string_view name)
{
    Cursor cursor;
    for (const auto & input : inputs)
        cursor[input.key];
    if (!tree)
        return cursor;

    auto parse_partitions = [&](const String & key, const CursorTreeNode & partitions)
    {
        for (const auto & [partition_id, value] : partitions)
        {
            const auto * position = std::get_if<CursorTreeNodePtr>(&value);
            if (!position || !(*position)->hasValue("block_number"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The {} has no block number for partition {} of table {}", name, partition_id, key);
            if ((*position)->getValue("block_offset", -1) != -1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The {} ends inside a block of partition {} of table {}", name, partition_id, key);
            if (!tables.at(key).isAbsent(partition_id))
                cursor[key][partition_id] = (*position)->getValue("block_number");
        }
    };

    for (const auto & [key, value] : *tree)
    {
        if (key == time_key && std::holds_alternative<Int64>(value))
            continue;
        const auto * partitions = std::get_if<CursorTreeNodePtr>(&value);
        if (!partitions || !cursor.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The {} has key {}, which is not the UUID of a table of the query", name, key);
        parse_partitions(key, **partitions);
    }
    return cursor;
}

String readShards(const String & cluster, const String & columns, const String & query)
{
    return fmt::format("SELECT {} FROM cluster({}, view({})) SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 0",
        columns, quoteString(cluster), query);
}

String makeShardCursor(const CursorTreeNodePtr & cursor)
{
    Strings entries;
    for (const auto & entry : cursor ? cursorTreeToMap(cursor) : Map{})
    {
        const auto & pair = entry.safeGet<Tuple>();
        entries.push_back(fmt::format("{}, {}", quoteString(pair.at(0).safeGet<String>()), pair.at(1).safeGet<Int64>()));
    }
    return fmt::format("mapApply((key, value) -> (substring(key, length(toString(shardNum())) + 2), value), "
        "mapFilter((key, value) -> startsWith(key, concat(toString(shardNum()), '.')), CAST(map({}), 'Map(String, Int64)')))",
        fmt::join(entries, ", "));
}

}

void checkReadableAsOf(const MergeTreeData & storage, const DataPartsVector & parts, const std::map<String, Int64> & bounds)
{
    for (const auto & part : parts)
    {
        if (part->info.isPatch() && storage.supportsReplication())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support replicated table {}, which has lightweight updates",
                storage.getStorageID().getNameForLogs());
        const auto range = getBlockRange(*part);
        /// The last update or mutation that the part holds. A merged part of a table whose merges rewrite rows, like a stage
        /// table, combines the rows of all its blocks.
        Int64 last = part->info.isPatch() ? range.max_block : part->info.mutation;
        if (!part->info.isPatch() && storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary)
            last = std::max(last, range.max_block);
        const Int64 bound = getBound(bounds, range.getPartitionId());
        if (range.min_block < bound && bound <= last)
            throw Exception(ErrorCodes::DELTA_QUERY_HISTORY_LOST,
                "Part {} of table {} no longer holds the rows as of block {} of partition {}, because a merge or a mutation wrote "
                "later changes into it", part->name, storage.getStorageID().getNameForLogs(), bound, range.getPartitionId());
    }
}

DataPartsVector getPatchParts(const MergeTreeData & storage, const std::map<String, Int64> & lower, const std::map<String, Int64> & upper)
{
    auto patches = storage.getPatchPartsVectorForInternalUsage();
    checkReadableAsOf(storage, patches, lower);
    checkReadableAsOf(storage, patches, upper);
    std::erase_if(patches, [&](const auto & patch)
    {
        const auto range = getBlockRange(*patch);
        return range.min_block < getBound(lower, range.getPartitionId()) || range.max_block >= getBound(upper, range.getPartitionId());
    });
    return patches;
}

CursorTreeNodePtr DeltaQuery::restrictCursor(const CursorTreeNodePtr & tree) const
{
    if (!tree)
        return nullptr;
    auto result = std::make_shared<CursorTreeNode>();
    for (const auto & input : inputs)
        if (tree->hasSubtree(input.key))
            result->setSubtree(input.key, tree->getSubtree(input.key));
    if (tree->hasValue(time_key))
        result->setValue(time_key, tree->getValue(time_key));
    return result;
}

CursorTreeNodePtr DeltaQuery::getFrontier(const CursorTreeNodePtr & from) const
{
    /// The cursor has the inputs of the base and of the steps, but not the table that holds `r`.
    if (recursion)
    {
        recursion->checkTable();
        auto tree = recursion->base->getFrontier(recursion->base->restrictCursor(from));
        for (const auto & step : recursion->steps)
            for (const auto & [key, value] : *step->getFrontier(step->restrictCursor(from)))
            {
                if (key == recursion->key || tree->hasSubtree(key) || tree->hasValue(key))
                    continue;
                if (const auto * node = std::get_if<CursorTreeNodePtr>(&value))
                    tree->setSubtree(key, *node);
                else
                    tree->setValue(key, std::get<Int64>(value));
            }
        return tree;
    }
    if (!cluster.empty())
    {
        auto tree = from ? from->clone() : std::make_shared<CursorTreeNode>();
        const auto shard_frontiers = readShards(cluster, "_shard_num, cursor", fmt::format("SELECT cursor FROM deltaQueryFrontier(({}), {})",
            shard_query->formatWithSecretsOneLine(), makeShardCursor(from)));
        InterpreterSelectQueryAnalyzer interpreter(parseSelect(shard_frontiers), analysis_context, SelectQueryOptions());
        auto io = interpreter.execute();
        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
            for (size_t row = 0; row < block.rows(); ++row)
                tree->setSubtree(toString((*block.getByPosition(0).column)[row].safeGet<UInt64>()),
                    buildCursorTree((*block.getByPosition(1).column)[row].safeGet<Map>()));
        return tree;
    }
    const auto tables = snapshotTables(inputs);
    auto frontier = parseCursor(from, inputs, tables, "cursor");
    for (const auto & [key, blocks] : tables)
    {
        auto & partitions = frontier.at(key);
        for (const auto & [partition_id, _] : blocks.getParts())
            partitions.try_emplace(partition_id, 0);
        for (auto & [partition_id, bound] : partitions)
            bound = blocks.getCommittedEnd(partition_id, bound);
        std::erase_if(partitions, [](const auto & partition) { return partition.second <= 0; });
    }
    auto tree = makeCursorTree(frontier);
    if (clock_input)
        tree->setValue(time_key, std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    return tree;
}

std::vector<StreamSubscriptionPtr> DeltaQuery::subscribeToCommits(const CursorTreeNodePtr & cursor, std::function<void()> callback) const
{
    if (recursion)
    {
        recursion->checkTable();
        auto subscriptions = recursion->base->subscribeToCommits(recursion->base->restrictCursor(cursor), callback);
        for (const auto & step : recursion->steps)
            std::ranges::move(step->subscribeToCommits(step->restrictCursor(cursor), callback), std::back_inserter(subscriptions));
        return subscriptions;
    }
    if (!cluster.empty())
        return {};
    /// The stages subscribe to the commits of their inputs. A stage table commits only when a refresh runs the stage.
    std::vector<StreamSubscriptionPtr> subscriptions;
    for (const auto & stage : stages)
        std::ranges::move(stage.query->subscribeToCommits(cursor && cursor->hasSubtree(stage.getCursorKey())
            ? cursor->getSubtree(stage.getCursorKey()) : nullptr, callback), std::back_inserter(subscriptions));
    std::vector<Input> tables_inputs;
    std::ranges::copy_if(inputs, std::back_inserter(tables_inputs), [](const auto & input) { return !input.stage; });
    CursorTreeNodePtr tables_cursor = cursor && !stages.empty() ? std::make_shared<CursorTreeNode>() : cursor;
    for (const auto & input : tables_inputs)
        if (tables_cursor != cursor && cursor->hasSubtree(input.key))
            tables_cursor->setSubtree(input.key, cursor->getSubtree(input.key));
    const auto tables = snapshotTables(tables_inputs);
    const auto bounds = parseCursor(tables_cursor, tables_inputs, tables, "cursor");
    std::set<String> keys;
    for (const auto & input : tables_inputs)
    {
        if (input.lookup || !keys.insert(input.key).second)
            continue;
        /// The subscription starts at the last block of each partition that the cursor includes, so it runs the callback only
        /// for blocks after the cursor.
        auto subscription = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
        std::map<std::string, int64_t> last_blocks;
        for (const auto & [partition_id, bound] : bounds.at(input.key))
            last_blocks[partition_id] = bound - 1;
        subscription->update(std::move(last_blocks), {});
        subscription->setCallback(callback);
        input.storage->subscription_manager.registerSubscription(subscription);
        input.storage->triggerStreamingSubscriptionEnrichment();
        subscriptions.push_back(std::move(subscription));
    }
    return subscriptions;
}

std::vector<scope_guard> DeltaQuery::holdCursor(const CursorTreeNodePtr & cursor) const
{
    if (recursion)
    {
        auto guards = recursion->base->holdCursor(recursion->base->restrictCursor(cursor));
        for (const auto & step : recursion->steps)
            std::ranges::move(step->holdCursor(step->restrictCursor(cursor)), std::back_inserter(guards));
        return guards;
    }
    if (!cluster.empty())
        return {};
    const auto tables = snapshotTables(inputs);
    const auto bounds = parseCursor(cursor, inputs, tables, "cursor");
    std::vector<scope_guard> guards;
    std::set<String> keys;
    for (const auto & input : inputs)
        if (auto storage = std::dynamic_pointer_cast<StorageMergeTree>(input.storage); storage && !input.lookup && keys.insert(input.key).second)
            guards.push_back(storage->holdDeltaQueryBounds(bounds.at(input.key)));
    return guards;
}

}
