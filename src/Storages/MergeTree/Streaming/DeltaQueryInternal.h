#pragma once

#include <Storages/MergeTree/Streaming/DeltaQuery.h>

#include <Core/Field.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>

#include <map>


/// The types and helpers that the files of `DeltaQuery` share.
namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ASTSelectQuery;

/// One table of the FROM clause of the query.
struct DeltaQuery::Input
{
    /// The unique alias of the table in the query, which qualifies its columns.
    String alias;
    /// The key of the table in cursors: its UUID.
    String key;
    std::shared_ptr<MergeTreeData> storage;
};

namespace DeltaQueryInternal
{

using Input = DeltaQuery::Input;

/// Table UUID -> partition ID -> first excluded block number.
using Cursor = std::map<String, std::map<String, Int64>>;

/// The queries whose deltas must merge into the query over the union of their inputs. Error messages name them.
constexpr std::string_view linear_queries = "delta queries";

/// The name of the sign column of the deltas of a query without `delta_query_sign_column`.
constexpr auto internal_sign_column = "__delta_query_sign";

Int64 getBound(const std::map<String, Int64> & partitions, const String & partition_id);

/// The parts of one table on this server and the promoters that report the blocks in flight to it.
class TableBlocks
{
public:
    explicit TableBlocks(MergeTreeData & storage)
    {
        if (!storage.getPatchPartsVectorForInternalUsage().empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, which has lightweight updates",
                storage.getStorageID().getNameForLogs());
        for (const auto & part : storage.getDataPartsVectorForInternalUsage())
        {
            if (part->info.mutation != 0)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, which has mutated part {}",
                    storage.getStorageID().getNameForLogs(), part->name);
            blocks[part->info.getPartitionId()].push_back(part->info);
        }
        /// Built after the parts snapshot, the promoters report a block committed in between as in flight.
        promoters = storage.buildPromoters();
    }

    /// The block ranges of each partition.
    const LocalPartsByPartition & getParts() const { return blocks; }

    /// Whether this server has no parts of the partition and no parts in flight to it, as after the partition is dropped.
    bool isAbsent(const String & partition_id) const { return !blocks.contains(partition_id) && !promoters.contains(partition_id); }

    /// The first block number from `start` on that this server has not committed to the partition.
    Int64 getCommittedEnd(const String & partition_id, Int64 start) const
    {
        auto partition_blocks = blocks.find(partition_id);
        auto promoter = promoters.find(partition_id);
        if (partition_blocks == blocks.end() || promoter == promoters.end())
            return start;
        return promoteBlockNumber(start - 1, partition_blocks->second, promoter->second) + 1;
    }

    /// Whether some partition has rows with `lower` <= `_block_number` < `upper`.
    bool hasBlocks(const std::map<String, Int64> & lower, const std::map<String, Int64> & upper) const
    {
        return std::ranges::any_of(blocks, [&](const auto & partition)
        {
            const auto & [partition_id, infos] = partition;
            return std::ranges::any_of(infos, [&](const auto & info)
            {
                return info.max_block >= getBound(lower, partition_id) && info.min_block < getBound(upper, partition_id);
            });
        });
    }

private:
    LocalPartsByPartition blocks;
    CursorPromotersMap promoters;
};


[[noreturn]] void throwUnsupported(std::string_view construct);

/// The name of the aggregate without its combinators, and the names of its combinators from the outermost.
std::pair<String, std::vector<String>> splitCombinators(const String & name);

/// Aggregates that `makeWeightedAggregate` rewrites. Their results are sums over rows. A row of weight -1 cancels its term.
bool isInvertibleAggregate(const String & name);

/// Aggregates whose results over disjoint inputs merge into the result over their union. A join term without GROUP BY
/// returns no row when it reads no rows. Only the groups of the terms then need to merge. A group can have no rows that
/// pass the condition of an `-If` form, and `minIf` or `maxIf` then returns 0. So only `sum` and `count` take `-If`.
bool isMergeableAggregate(const String & name);

/// `qualifier.column`, or `column` for an empty qualifier.
ASTPtr makeColumn(const String & qualifier, const String & column);

/// `c_1 AND ...`, or `1` without conjuncts.
ASTPtr makeConjunction(ASTs conjuncts);

/// Adds `SETTINGS name = value` to `select`, keeping its other settings.
void setSetting(ASTSelectQuery & select, std::string_view name, const Field & value);

std::map<String, TableBlocks> snapshotTables(const std::vector<Input> & inputs);

/// Parses `tree` and removes the partitions that this server has no parts of and no parts in flight to. Reading such a
/// partition returns no rows below any bound, so removing it changes no result. It also lets the partition start over
/// if it gets rows again.
Cursor parseCursor(const CursorTreeNodePtr & tree, const std::vector<Input> & inputs, const std::map<String, TableBlocks> & tables,
    std::string_view name);


/// Conjuncts that keep the rows of an input with `lower` <= `_block_number` < `upper`, where a null `lower` keeps every row
/// below `upper`. The `indexHint` on `mergeTreePartInfo(_part)` skips the parts outside the bounds before reading them.
/// Rows don't evaluate it.
void addBounds(ASTs & conjuncts, const String & qualifier, const std::map<String, Int64> * lower,
    const std::map<String, Int64> & upper);

}

}
