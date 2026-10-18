#pragma once

#include <Core/Streaming/CursorTree.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <vector>


namespace DB
{

/// Evaluates a SELECT over MergeTree tables on the rows inserted between two cursors. The delta of a query over one table
/// is the query over the new rows of the table. The delta of a query that reads several tables has one term per table
/// occurrence, which joins the new rows of that occurrence with the rows of the others. Such a query must be linear, so
/// that merging the deltas over a chain of cursors gives the query evaluated at the last cursor.
///
/// A cursor is a `CursorTree` with the layout of a `STREAM` cursor under one more level for the table:
/// table UUID -> partition ID -> `block_number`. The value is the first block number that the cursor excludes. So the
/// table as of a cursor is the rows with `_block_number` below the value, and a missing partition or table has no rows.
///
/// `deltaQuery` and incremental refreshable materialized views use this class.
class DeltaQuery
{
public:
    /// Throws unless `query` is supported. Resolving `query` also checks the grants of `context` on its tables.
    DeltaQuery(const ASTPtr & query, const ContextPtr & context);
    ~DeltaQuery();

    /// The cursor `from` extended by the blocks that this server has committed since. A null `from` is the empty cursor.
    CursorTreeNodePtr getFrontier(const CursorTreeNodePtr & from) const;

    /// The delta between `old_cursor` and `new_cursor`. A null `old_cursor` is the empty cursor. Throws unless
    /// `old_cursor` <= `new_cursor` and this server has committed every block that the delta reads.
    ASTPtr buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const;

    struct Input;

private:
    ASTPtr query;
    std::vector<Input> inputs;
};

}
