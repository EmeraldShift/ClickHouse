#pragma once

#include <Core/Names.h>
#include <Core/NamesAndTypes.h>
#include <Core/Streaming/CursorTree.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>
#include <vector>


namespace DB
{

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
/// `deltaQuery` and `REFRESH ... INCREMENTAL` views use this class.
class DeltaQuery
{
public:
    /// Throws unless `query` is supported and the grants of `context` allow running it.
    DeltaQuery(const ASTPtr & query, const ContextPtr & context);
    ~DeltaQuery();

    /// The cursor `from` extended by the blocks that this server has committed since. A null `from` is the empty cursor.
    CursorTreeNodePtr getFrontier(const CursorTreeNodePtr & from) const;

    /// The delta between `old_cursor` and `new_cursor`. A null `old_cursor` is the empty cursor. Throws unless
    /// `old_cursor` <= `new_cursor` and this server has committed every block that the delta reads.
    ASTPtr buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const;

    /// A query with the result columns of the rows that the deltas insert into a target. That is the query that each delta
    /// evaluates over part of the inputs.
    ASTPtr getQuery() const;

    /// The columns of the rows that the deltas insert into a target, see `getQuery`.
    NamesAndTypesList getResultColumns() const;

    /// Throws if the rows of the query have a sign without `delta_query_sign_column`. Without the setting, the deltas weigh
    /// their rows by a sign column of an internal name, and a target can't have that column.
    void checkResultSign() const;

    struct Input;
    struct SemiJoinEdge;

private:
    /// Analyzes `query_` into `query` and `inputs`.
    void analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column);

    struct DeltaTerms;

    /// The resolved query, whose tables have unique aliases.
    ASTPtr query;
    std::vector<Input> inputs;
    String sign_column;
    /// The input that the filters by now() read, and those filters, which are conjuncts of WHERE.
    std::optional<size_t> clock_input;
    ASTs clock_conjuncts;
    /// The result columns of a signed query without aggregates, without the sign column. A delta with pairs of terms sums
    /// the signs of equal rows.
    Names consolidated_columns;
    /// The context of the analysis. It also reads the types of the result columns.
    ContextPtr analysis_context;
};

}
