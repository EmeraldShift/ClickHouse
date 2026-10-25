#pragma once

#include <Storages/MergeTree/Streaming/DeltaQuery.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Core/Field.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>

#include <functional>
#include <map>
#include <set>


/// The types and helpers that the files of `DeltaQuery` share.
namespace DB
{

namespace ErrorCodes
{
    extern const int UNFINISHED;
}

class ASTSelectQuery;
struct ASTTableExpression;
class QueryNode;

/// One table of the FROM clause of the query.
struct DeltaQuery::Input
{
    /// The unique alias of the table in the query, which qualifies its columns.
    String alias;
    /// The key of the table in cursors: its UUID.
    String key;
    std::shared_ptr<MergeTreeData> storage;
    /// Whether `delta_query_lookup_tables` names the table. Its new rows then have no term, and the other terms read its
    /// rows below the new cursor.
    bool lookup = false;
    bool is_signed = false;
    /// Whether lightweight updates can change the rows of the table. Delta queries read them from plain `MergeTree` tables,
    /// which have them with `enable_block_offset_column`.
    bool updatable = false;
    /// The columns of the table in the equalities of the ON clause, if the table is the right side of a LEFT, SEMI, ANTI or
    /// ASOF join. Such a join isn't linear in the table. A new row of the table changes the result for every left row of
    /// its join keys, see `buildDelta`.
    Names join_keys = {};
    /// The columns of such a table that the query reads.
    Names columns = {};
};

/// An equality `from.from_column = to.to_column` between join keys of two inputs. The semi-join reduction follows it from
/// input `from` to input `to`.
struct DeltaQuery::SemiJoinEdge
{
    size_t from;
    String from_column;
    size_t to;
    String to_column;
    /// The type of `from_column` in the query.
    String from_type;
    /// Whether the equality is in the ON of a LEFT, SEMI, ANTI or ASOF join with `from` on the right and `to` on the left.
    bool to_left_side = false;

    bool operator==(const SemiJoinEdge &) const = default;
};

/// An ASOF join `... ON l.k_1 = r.k_1 AND ... AND l.t >= r.t` whose ON has only those conjuncts, where input `left` has
/// every column on the left. The match of a left row is the latest right row of its keys at or before its time. For `<=`
/// and `<` it is the earliest one at or after its time.
struct DeltaQuery::AsofJoin
{
    size_t left = 0;
    size_t right = 0;
    /// The columns of the equalities, in the same order for both inputs.
    Names left_keys;
    Names right_keys;
    String left_time;
    String right_time;
    /// Whether the match is at or before the time of the left row, for `>=` and `>`.
    bool backward = true;
    /// Whether the match can't have the time of the left row, for `>` and `<`.
    bool strict = false;
    /// The lookback, see `makeAsofLookback`.
    ASTPtr lookback;
};

namespace DeltaQueryInternal
{

using Input = DeltaQuery::Input;

using SemiJoinEdge = DeltaQuery::SemiJoinEdge;

/// Table UUID -> partition ID -> first excluded block number.
using Cursor = std::map<String, std::map<String, Int64>>;

/// The queries whose deltas must merge into the query over the union of their inputs. Error messages name them.
constexpr std::string_view linear_queries = "delta queries";

/// The name of the sign column of the deltas of a query without `delta_query_sign_column`.
constexpr auto internal_sign_column = "__delta_query_sign";

/// The factor of the weights of queries whose deltas have terms that retract rows, see `DeltaQuery::buildDelta`.
constexpr auto term_sign_name = "__term_sign";

Int64 getBound(const std::map<String, Int64> & partitions, const String & partition_id);

/// The parts of one table on this server, the promoters that report the blocks in flight to it, and its unfinished
/// mutations.
class TableBlocks
{
public:
    explicit TableBlocks(MergeTreeData & storage_)
        : storage(storage_)
        , parts(storage.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active},
            {MergeTreeData::DataPartKind::Regular, MergeTreeData::DataPartKind::Patch}))
        , blocks(getLocalParts(parts))
    {
        DB::checkReadableAsOf(storage, parts, {});
        /// Built after the parts snapshot, the promoters report a block committed in between as in flight.
        promoters = storage.buildPromoters();
        for (const auto & status : storage.getMutationsStatus())
            if (!status.is_done)
                for (const auto & [partition_id, block_number] : status.block_numbers)
                    unfinished_mutations.push_back({status.id, partition_id, block_number});
    }

    /// The block ranges of each partition, see `getLocalParts`.
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

    /// Whether some partition has rows with `lower` <= `_block_number` < `upper`, or for `updates`, lightweight updates
    /// with versions in that range. A patch part spans no bound that passes `checkReadableAsOf`.
    bool hasBlocks(const std::map<String, Int64> & lower, const std::map<String, Int64> & upper, bool updates) const
    {
        return std::ranges::any_of(parts, [&](const auto & part)
        {
            const auto range = getBlockRange(*part);
            return part->info.isPatch() == updates && range.max_block >= getBound(lower, range.getPartitionId())
                && range.min_block < getBound(upper, range.getPartitionId());
        });
    }

    /// Throws unless the parts hold the rows of the table as of `bounds` and the mutations below `bounds` are done. A mutation
    /// of a plain `MergeTree` table has one block number for every partition, under the empty partition ID.
    void checkReadableAsOf(const std::map<String, Int64> & bounds) const
    {
        DB::checkReadableAsOf(storage, parts, bounds);
        for (const auto & [id, partition_id, block_number] : unfinished_mutations)
            for (const auto & [bound_partition_id, bound] : bounds)
                if ((partition_id.empty() || partition_id == bound_partition_id) && block_number < bound)
                    throw Exception(ErrorCodes::UNFINISHED, "Delta queries wait for mutation {} of table {}, which is below block {} of partition {}",
                        id, storage.getStorageID().getNameForLogs(), bound, bound_partition_id);
    }

private:
    struct Mutation
    {
        String id;
        String partition_id;
        Int64 block_number;
    };

    MergeTreeData & storage;
    DataPartsVector parts;
    LocalPartsByPartition blocks;
    CursorPromotersMap promoters;
    std::vector<Mutation> unfinished_mutations;
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

bool isAggregate(const IAST & ast);

/// The table identifier, table function or subquery of `expression`.
ASTPtr & getTable(ASTTableExpression & expression);

/// The table expression in FROM of `select` whose table passes `matches`, or null.
ASTTableExpression * findTable(const ASTSelectQuery & select, const std::function<bool(const IAST & table)> & matches);

/// The table expression in FROM of `select` of the table with alias `alias`.
ASTTableExpression & findAliasedTable(const ASTSelectQuery & select, const String & alias);

/// Replaces the table of `expression` by `table`, a table identifier, a table function or a subquery.
void setTable(ASTTableExpression & expression, ASTPtr table);

void splitConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts);

/// Parses `sql`, whose unions have explicit modes.
ASTPtr parseSelect(const String & sql);

ASTPtr makeSelectWithUnion(ASTPtr select);

/// `query_1 UNION ALL ...` over selects and unions of selects. The unions of delta queries are UNION ALL, so the selects of
/// the unions in `queries` become selects of the result.
ASTPtr makeUnionAll(const ASTs & queries);

/// `(query)` for a select or a union.
ASTPtr makeSubquery(ASTPtr query);

/// The FROM clause of `table`, a table identifier, a table function or a subquery.
ASTPtr makeTables(ASTPtr table);

/// `SELECT columns FROM table WHERE where GROUP BY group_by HAVING having`, without the clauses that are null or empty.
ASTPtr makeSelect(ASTs columns, ASTPtr table, ASTPtr where = nullptr, ASTs group_by = {}, ASTPtr having = nullptr);

/// `ast AS alias`.
ASTPtr withAlias(ASTPtr ast, const String & alias);

ASTs makeIdentifiers(const Names & names);

/// `qualifier.column`, or `column` for an empty qualifier.
ASTPtr makeColumn(const String & qualifier, const String & column);

/// The one element of `elements`, or else `tuple(elements...)`.
ASTPtr makeTuple(ASTs elements);

/// `c_1 AND ...`, or `1` without conjuncts.
ASTPtr makeConjunction(ASTs conjuncts);

ASTs cloneAll(const ASTs & asts);

/// The names of the identifiers and the aliases in `ast`.
void collectNames(const IAST & ast, NameSet & names);

/// `base`, or else `base_1`, `base_2` and so on, whichever `taken` doesn't have first.
String makeUniqueName(const String & base, const NameSet & taken);

/// Replaces the identifier `__term_sign` by `sign` and the calls of now() by `time`.
void bindTerm(IAST & ast, std::optional<Int64> sign, std::optional<Int64> time);

bool callsNow(const IAST & ast);

/// The columns that `ast` reads by qualified identifiers, by qualifier.
void collectColumns(const IAST & ast, std::map<String, std::set<String>> & columns);

/// Adds `SETTINGS name = value` to `select`, keeping its other settings.
void setSetting(ASTSelectQuery & select, std::string_view name, const Field & value);

using Visit = std::function<ASTPtr(const ASTPtr & node)>;

/// Calls `visit` on the nodes of the expression `node` of `parent` top-down. A node that `visit` returns replaces `node`,
/// and its children aren't visited. Neither are the identifiers of lambda parameters.
void visitExpression(ASTPtr & node, IAST & parent, const Visit & visit, std::multiset<String> & lambda_parameters);

/// `CAST(column, type) AS column` with the type of `column` in `types`.
ASTPtr castTo(const String & column, const NamesAndTypesList & types);

/// The rows of `table` that pass `where`, if not null, with the values `values` of the columns of `consolidate` and the
/// weight `weight`.
struct WeightedRows
{
    ASTPtr table;
    ASTs values;
    ASTPtr weight;
    ASTPtr where = nullptr;
};

/// The rows of `sources` as rows of `columns` with sign 1 or -1 in column `sign`. Equal rows sum their weights first, so rows
/// of opposite weights cancel.
ASTPtr consolidate(const Names & columns, const String & sign, std::vector<WeightedRows> sources);

std::map<String, TableBlocks> snapshotTables(const std::vector<Input> & inputs);

std::optional<Int64> getTime(const CursorTreeNodePtr & tree);

/// Parses `tree` and removes the partitions that this server has no parts of and no parts in flight to. Reading such a
/// partition returns no rows below any bound, so removing it changes no result. It also lets the partition start over
/// if it gets rows again.
Cursor parseCursor(const CursorTreeNodePtr & tree, const std::vector<Input> & inputs, const std::map<String, TableBlocks> & tables,
    std::string_view name);

/// `SELECT columns FROM cluster(cluster, view(query))`. It runs `query` on one replica of each shard, and all shards must
/// run it. This server runs its own shard over a connection too. Then `shardNum()` in `query` is the number of the shard.
/// A local read would reuse the table functions of `query` that this server evaluates for the header, with `shardNum()` 0.
String readShards(const String & cluster, const String & columns, const String & query);

/// The expression that each shard evaluates to its cursor in `cursor`, the subtree of its number.
String makeShardCursor(const CursorTreeNodePtr & cursor);


/// Conjuncts that keep the rows of an input with `lower` <= `_block_number` < `upper`, where a null `lower` keeps every row
/// below `upper`. The `indexHint` on `mergeTreePartInfo(_part)` skips the parts outside the bounds before reading them.
/// Rows don't evaluate it.
void addBounds(ASTs & conjuncts, const String & qualifier, const std::map<String, Int64> * lower,
    const std::map<String, Int64> & upper);

/// The table of `input` with the lightweight updates below `bounds`, `deltaQuerySnapshot('database', 'table', map(...))`,
/// or the table itself if it can't have updates.
ASTPtr makeSnapshot(const Input & input, const std::map<String, Int64> & bounds);

/// Makes the rows of `table` that pass `conjuncts` the rows of `input` in `select`, whose WHERE gets `where`. The conjuncts
/// go to `where`, except for the right side of a LEFT, SEMI, ANTI or ASOF join, which needs its bounds before the join. In
/// WHERE they would filter the joined rows, and a left row whose matches are all out of bounds would get no row instead of
/// a row without a match. So the table of the right side becomes the subquery of those rows. Over a signed table the
/// subquery sums the signs of equal rows, because the join must see which rows remain, not each insert and retraction.
void setInput(ASTSelectQuery & select, const Input & input, ASTPtr table, ASTs conjuncts, ASTs & where, const String & sign_column);

}

}
