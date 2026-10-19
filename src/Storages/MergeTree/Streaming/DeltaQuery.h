#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Streaming/CursorTree.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/Streaming/IStreamSubscription.h>
#include <base/scope_guard.h>
#include <functional>

#include <map>
#include <memory>
#include <optional>
#include <vector>


namespace DB
{

class MergeTreeData;
class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

/// Evaluates a SELECT over MergeTree tables on the rows inserted between two cursors. The delta of a query over one table
/// is the query over the new rows of the table. The delta of a query that reads several tables has one term per table
/// occurrence, which joins the new rows of that occurrence with the rows of the others. Such a query must be linear, so
/// that merging the deltas over a chain of cursors gives the query evaluated at the last cursor.
///
/// With `delta_query_sign_column`, each input that has that column is signed. A row of a signed input with sign 1 inserts
/// the row, and a row with sign -1 retracts an earlier row with the same values. The weight of a joined row is the product
/// of the signs of its signed inputs. A query without aggregates returns the weight of each row as that column. A query
/// with aggregates weighs its rows in its aggregates. The deltas merge like the deltas of a join.
///
/// A query can also have LEFT, SEMI, ANTI and ASOF joins of a table, and filters by now() in WHERE. Their deltas retract
/// rows, so they need `delta_query_sign_column`. See `buildDelta`.
///
/// With `delta_query_sign_column`, a query can also have a residual: the clauses whose result over the union of the deltas
/// differs from the union of their results, like HAVING, ORDER BY with LIMIT, LIMIT BY, DISTINCT, window functions,
/// expressions over aggregates and aggregates like `uniq`. The query then splits into a linear core and the residual. The
/// core groups the rows by keys, which are the GROUP BY keys or else the columns that the residual reads, and its deltas
/// have the form above. An integral table sums the deltas of the core. Each refresh evaluates the residual over the rows of
/// the integral whose keys the delta touched, and diffs the result with the rows that the target has for those keys. The
/// rows of the diff have the sign column, so the target can be a `CollapsingMergeTree`.
///
/// A query can also be `WITH RECURSIVE r AS (base UNION ALL step ...) SELECT * FROM r`, where each step reads `r` once in
/// FROM through inner joins. The steps are linear in `r`, so the delta of `r` is the least fixed point of `d = d_0 + step(d)`.
/// Here `d_0` is the delta of the base plus the delta of each step over the rows of `r` at the old cursor, and `step(d)` reads
/// the inputs at the new cursor. The rows of `r` at the old cursor come from `delta_query_recursion_table`, which an
/// incremental refreshable materialized view sets to its target. The weights of the rows make this hold for retractions too.
///
/// A cursor is a `CursorTree` with the layout of a `STREAM` cursor under one more level for the table:
/// table UUID -> partition ID -> `block_number`. The value is the first block number that the cursor excludes. So the
/// table as of a cursor is the rows with `_block_number` below the value, and a missing partition or table has no rows.
/// The cursor of a query that calls now() also has the key `now`, the time of the snapshot in seconds.
///
/// A query can also read `Distributed` tables of one cluster. Each shard then runs the query over its local tables, as
/// with `distributed_product_mode = 'local'`. So the rows that the query joins must be on the same shard. Each shard reads
/// the other tables of the query itself. The analysis reads the local tables of this server. A cursor maps the number of
/// each shard to the cursor of the query of the shard.
///
/// A lightweight update of a `MergeTree` table takes a block number too, and the table as of a cursor has the updates
/// below it. The delta reads the rows that the updates in between change as of both cursors, see `buildDelta`. Merges
/// that write later updates into older rows lose the rows as of a cursor, and the delta then throws
/// `DELTA_QUERY_HISTORY_LOST`, see `checkReadableAsOf`.
///
/// The query of an incremental refreshable materialized view can also read subqueries in FROM. Each subquery is a stage.
/// A stage is a delta query of its own, and its deltas go to a hidden stage table of the view. The query reads the stage
/// table as a signed input. A stage with aggregates has a residual even if its aggregates are sums. So a refresh retracts
/// the old row of each group that it changes and inserts the new row. The stage table then holds the rows of the subquery
/// with signs. The cursor of a query with stages has the cursor of each stage under the key of the stage, see `Stage`.
///
/// `deltaQuery` and incremental refreshable materialized views use this class.
class DeltaQuery
{
public:
    /// Throws unless `query` is supported and the grants of `context` allow running it. Subqueries in FROM need a `view`.
    /// The view names their stage tables. A non-empty `stage_path` makes the query the stage of that path of `view`.
    DeltaQuery(const ASTPtr & query, const ContextPtr & context, const StorageID & view = StorageID::createEmpty(),
        const String & stage_path = {});
    ~DeltaQuery();

    /// A subquery in FROM and its stage table. The path of the i-th subquery in FROM of the query of a view is `i`. The path
    /// of the i-th subquery in FROM of the stage of path `p` is `p.i`.
    struct Stage
    {
        String path;
        StorageID table;
        std::unique_ptr<DeltaQuery> query;

        /// The key of the cursor of the stage in the cursor of the query that reads the stage.
        String getCursorKey() const { return "stage." + path; }
    };
    const std::vector<Stage> & getStages() const { return stages; }
    std::vector<Stage> & getStages() { return stages; }

    /// The stage tables of the subqueries of `query` of `view` and of their subqueries. Doesn't analyze `query`.
    static std::vector<StorageID> getStageTables(const ASTPtr & query, const StorageID & view);

    /// `CREATE TABLE IF NOT EXISTS` of the stage table `table` of this stage. The stage table weighs its rows by an Int64
    /// sign. Its merges sum the signs of equal rows unless a column can't be in a sorting key.
    String getStageCreateQuery(const StorageID & table) const;

    /// Makes the inputs of the stages read the stage tables, which must exist. The frontier and the delta need them.
    void bindStageTables(const ContextPtr & context);

    /// The cursor `from` extended by the blocks that this server has committed since. A null `from` is the empty cursor.
    /// For a query over `Distributed` tables, each shard extends its cursor.
    CursorTreeNodePtr getFrontier(const CursorTreeNodePtr & from) const;

    /// Runs `callback` each time an input commits blocks after `cursor`, on a background thread of the input. Lookup tables
    /// don't run it, because their new rows don't change the result. Destroying the subscriptions stops the calls. A query
    /// over `Distributed` tables gets no subscriptions, because its inputs are on the shards.
    std::vector<StreamSubscriptionPtr> subscribeToCommits(const CursorTreeNodePtr & cursor, std::function<void()> callback) const;

    /// Keeps the merges of the inputs from losing their rows as of `cursor` while the guards live, so that the delta from
    /// `cursor` can read them, see `StorageMergeTree::holdDeltaQueryBounds`. The delta reads lookup tables only as of its new
    /// cursor, so they have no guards. Nor do the inputs of a query over `Distributed` tables, which are on the shards.
    std::vector<scope_guard> holdCursor(const CursorTreeNodePtr & cursor) const;

    /// The delta between `old_cursor` and `new_cursor`. A null `old_cursor` is the empty cursor. Throws unless
    /// `old_cursor` <= `new_cursor` and this server has committed every block that the delta reads.
    ASTPtr buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const;

    /// A query with the result columns of the rows that the deltas insert into a target. That is the query that each delta
    /// evaluates over part of the inputs, or for a query with a residual, the residual with the sign column.
    ASTPtr getQuery() const;

    /// Whether the query has a residual. `buildDelta` then returns the deltas of the core, which go to the integral.
    bool hasResidual() const { return residual != nullptr; }

    /// Whether the residual needs no integral, because the target holds all of its state. See `Residual::append_only`.
    bool isAppendOnly() const;

    /// `CREATE TABLE IF NOT EXISTS` of the integral of the core.
    String getIntegralCreateQuery(const StorageID & integral) const;

    /// Whether the residual compares rows only within the touched values of some keys, like HAVING, LIMIT BY or a window
    /// with PARTITION BY. A refresh can then recompute the residual for the touched values or for all keys.
    bool hasPartitionedResidual() const;

    /// The rows that make `target` equal to the residual over `integral`, after `integral` absorbed the delta of the core
    /// in table `delta`. The rows have the sign column. The residual runs over the keys that the delta touched, unless
    /// `all_keys` is set or the residual can move rows between keys, like a LIMIT over all rows. Then it runs over all keys.
    /// An empty `delta` touches no keys. An append-only residual runs over the rows of `target` and `delta` instead of
    /// `integral`.
    ASTPtr buildResidualDelta(const StorageID & integral, const StorageID & delta, bool delta_is_empty, const StorageID & target,
        bool all_keys = false) const;

    /// Whether rows can make a target that holds the deltas of the query equal to the query as of a cursor. A signed query
    /// allows that. A recursive query doesn't, nor does a query with an append-only residual or with aggregates that aren't
    /// all sums. For a query with a residual, `buildResidualDelta` over all keys returns those rows after the integral absorbs
    /// the delta from the empty cursor. For the others, `buildCorrection` returns them.
    bool canCorrect() const;

    /// The rows that make `target` equal to the query as of `new_cursor`, for a query without a residual. They retract the
    /// rows of `target` and add the rows of the query. Rows that cancel each other are left out.
    ASTPtr buildCorrection(const CursorTreeNodePtr & new_cursor, const StorageID & target) const;

    /// The columns of the rows that the deltas insert into a target, see `getQuery`.
    NamesAndTypesList getResultColumns() const;

    /// The columns of the stage table of this stage, see `getStageCreateQuery`.
    NamesAndTypesList getStageColumns() const;

    /// The entries of `tree` for the inputs of this query, and its time.
    CursorTreeNodePtr restrictCursor(const CursorTreeNodePtr & tree) const;

    struct Input;
    struct SemiJoinEdge;
    struct Residual;
    struct Recursion;

private:
    /// Analyzes `query_` into `query`, `inputs` and `semi_join_edges`. With a non-null `split`, a query that has a residual
    /// fills `split` and returns its core instead, which the caller analyzes.
    ASTPtr analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column, Residual * split);

    /// `CAST(c, type) AS c, ...` of `columns` of a target, with the types of the rows that the deltas insert. A target can
    /// declare other types, like UInt64 for a count that the deltas weigh as Int64.
    String castToResultTypes(const Names & columns) const;

    /// The delta of a recursive query, see `Recursion`.
    ASTPtr buildRecursiveDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const;

    /// The query of a step of a recursive query at `new_cursor`, which reads CTE `cte` instead of the input of key `key`.
    ASTPtr buildRecursiveTerm(const CursorTreeNodePtr & new_cursor, const String & key, const String & cte) const;

    /// The resolved query, whose tables have unique aliases. For a query with a residual, its core.
    ASTPtr query;
    std::vector<Input> inputs;
    std::vector<SemiJoinEdge> semi_join_edges;
    std::shared_ptr<const Residual> residual;
    std::shared_ptr<const Recursion> recursion;
    std::vector<Stage> stages;
    /// Whether this query is a stage, see `Stage`.
    bool is_stage = false;
    String sign_column;
    /// The input that the filters by now() read, and those filters, which are conjuncts of WHERE.
    std::optional<size_t> clock_input;
    ASTs clock_conjuncts;
    /// The result columns of a signed query without aggregates, without the sign column. A delta with pairs of terms sums
    /// the signs of equal rows.
    Names consolidated_columns;
    /// The result columns of a query with aggregates over signed inputs that are all sums, like `count` and `sum`. The flag
    /// marks the aggregates. The rows of a target with equal other columns add up.
    std::vector<std::pair<String, bool>> summed_columns;
    /// For a query over `Distributed` tables, their cluster and the query that each shard runs over its local tables. For a
    /// query with a residual, that is the core.
    String cluster;
    ASTPtr shard_query;
    /// The context of the analysis. It also reads the frontiers of the shards and the types of the result columns.
    ContextPtr analysis_context;
};

/// Throws `DELTA_QUERY_HISTORY_LOST` unless `parts` of `storage` hold its rows as of `bounds`, the first block number that
/// each partition excludes. The table as of a bound has the rows below the bound with the lightweight updates below it.
/// A part that holds a mutation or an update at or above a bound, and rows below it, has lost the rows as of the bound.
/// So has a patch part whose updates span the bound, because a merge of patch parts keeps only the latest value of a row.
/// Throws `NOT_IMPLEMENTED` for the patch parts of a replicated table.
void checkReadableAsOf(const MergeTreeData & storage, const DataPartsVector & parts, const std::map<String, Int64> & bounds);

/// The patch parts of the lightweight updates of `storage` with versions from `lower` up to `upper`. Throws unless the
/// patch parts hold the updates as of both, see `checkReadableAsOf`.
DataPartsVector getPatchParts(const MergeTreeData & storage, const std::map<String, Int64> & lower, const std::map<String, Int64> & upper);

}
