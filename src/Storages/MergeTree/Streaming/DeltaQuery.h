#pragma once

#include <Core/Names.h>
#include <Core/Streaming/CursorTree.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/Streaming/IStreamSubscription.h>
#include <functional>

#include <memory>
#include <optional>
#include <vector>


namespace DB
{

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
/// A cursor is a `CursorTree` with the layout of a `STREAM` cursor under one more level for the table:
/// table UUID -> partition ID -> `block_number`. The value is the first block number that the cursor excludes. So the
/// table as of a cursor is the rows with `_block_number` below the value, and a missing partition or table has no rows.
/// The cursor of a query that calls now() also has the key `now`, the time of the snapshot in seconds.
///
/// `deltaQuery` and incremental refreshable materialized views use this class.
class DeltaQuery
{
public:
    /// Throws unless `query` is supported and the grants of `context` allow running it.
    DeltaQuery(const ASTPtr & query, const ContextPtr & context);
    ~DeltaQuery();

    /// The cursor `from` extended by the blocks that this server has committed since. A null `from` is the empty cursor.
    CursorTreeNodePtr getFrontier(const CursorTreeNodePtr & from) const;

    /// Runs `callback` each time an input commits blocks after `cursor`, on a background thread of the input. Lookup tables
    /// don't run it, because their new rows don't change the result. Destroying the subscriptions stops the calls.
    std::vector<StreamSubscriptionPtr> subscribeToCommits(const CursorTreeNodePtr & cursor, std::function<void()> callback) const;

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

    struct Input;
    struct SemiJoinEdge;
    struct Residual;

private:
    /// Analyzes `query_` into `query`, `inputs` and `semi_join_edges`. With a non-null `split`, a query that has a residual
    /// fills `split` and returns its core instead, which the caller analyzes.
    ASTPtr analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column, Residual * split);

    /// The resolved query, whose tables have unique aliases. For a query with a residual, its core.
    ASTPtr query;
    std::vector<Input> inputs;
    std::vector<SemiJoinEdge> semi_join_edges;
    std::shared_ptr<const Residual> residual;
    String sign_column;
    /// The input that the filters by now() read, and those filters, which are conjuncts of WHERE.
    std::optional<size_t> clock_input;
    ASTs clock_conjuncts;
    /// The result columns of a query without aggregates whose delta retracts rows, without the sign column. The delta sums
    /// the signs of equal rows.
    Names consolidated_columns;
};

}
