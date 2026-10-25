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

/// Evaluates a SELECT over MergeTree tables on the rows inserted between two cursors. The delta of a query has one term per
/// table occurrence. The term joins the new rows of that occurrence with the rows of the others. The query must be linear.
/// Then merging the deltas over a chain of cursors gives the query evaluated at the last cursor.
///
/// The deltas weigh their rows by a sign. With `delta_query_sign_column`, each input that has that column is signed. A row
/// of a signed input with sign 1 inserts the row, and a row with sign -1 retracts an earlier row with the same values. The
/// weight of a joined row is the product of the signs of its signed inputs. A query without aggregates returns the weight
/// of each row as that column. A query with aggregates weighs its rows in its aggregates. Without the setting, the sign
/// column has an internal name. A query whose rows then have a sign can't have a target, see `checkResultSign`.
///
/// A query can also have LEFT, SEMI, ANTI and ASOF joins of a table, and filters by now() in WHERE. Their deltas retract
/// rows. See `buildDelta`.
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
/// `deltaQuery` and `REFRESH ... INCREMENTAL` views use this class.
class DeltaQuery
{
public:
    /// Throws unless `query` is supported and the grants of `context` allow running it.
    DeltaQuery(const ASTPtr & query, const ContextPtr & context);
    ~DeltaQuery();

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
    /// evaluates over part of the inputs.
    ASTPtr getQuery() const;

    /// Whether rows can make a target that holds the deltas of the query equal to the query as of a cursor. A signed query
    /// allows that. A query with aggregates that aren't all sums doesn't. `buildCorrection` returns those rows.
    bool canCorrect() const;

    /// The rows that make `target` equal to the query as of `new_cursor`. They retract the rows of `target` and add the rows
    /// of the query. Rows that cancel each other are left out.
    ASTPtr buildCorrection(const CursorTreeNodePtr & new_cursor, const StorageID & target) const;

    /// The columns of the rows that the deltas insert into a target, see `getQuery`.
    NamesAndTypesList getResultColumns() const;

    /// Throws if the rows of the query have a sign without `delta_query_sign_column`. Without the setting, the deltas weigh
    /// their rows by a sign column of an internal name, and a target can't have that column.
    void checkResultSign() const;

    struct Input;
    struct SemiJoinEdge;
    struct AsofJoin;

private:
    /// Analyzes `query_` into `query`, `inputs` and `semi_join_edges`.
    void analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column);

    struct DeltaTerms;

    /// `CAST(c, type) AS c, ...` of `columns` of a target, with the types of the rows that the deltas insert. A target can
    /// declare other types, like UInt64 for a count that the deltas weigh as Int64.
    ASTs castToResultTypes(const Names & columns) const;

    /// The resolved query, whose tables have unique aliases.
    ASTPtr query;
    std::vector<Input> inputs;
    std::vector<SemiJoinEdge> semi_join_edges;
    /// The ASOF joins whose deltas read only the rows near the time of the changes, see `delta_query_asof_lookback`.
    std::vector<AsofJoin> asof_joins;
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
    /// For a query over `Distributed` tables, their cluster and the query that each shard runs over its local tables.
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
