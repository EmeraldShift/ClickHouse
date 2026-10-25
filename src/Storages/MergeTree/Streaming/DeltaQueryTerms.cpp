#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <map>
#include <set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

using namespace DeltaQueryInternal;

namespace
{

/// `transform(_partition_id, ids, bounds, 0)`, the bound of the partition of the row. A missing partition has bound 0.
ASTPtr makeBound(const String & qualifier, const std::map<String, Int64> & partitions)
{
    Array ids;
    Array bounds;
    for (const auto & [partition_id, bound] : partitions)
    {
        ids.push_back(partition_id);
        bounds.push_back(bound);
    }
    return makeASTFunction("transform",
        makeColumn(qualifier, "_partition_id"),
        make_intrusive<ASTLiteral>(std::move(ids)),
        make_intrusive<ASTLiteral>(std::move(bounds)),
        make_intrusive<ASTLiteral>(Int64(0)));
}

/// Throws unless `old_bounds` <= `new_bounds` and this server has committed every block that the delta reads. Term i reads
/// the blocks of input i in [old, new). In a join, the other terms read the blocks below old or new.
void checkCursors(const std::map<String, TableBlocks> & tables, const Cursor & old_bounds, const Cursor & new_bounds, bool is_join)
{
    for (const auto & [key, blocks] : tables)
    {
        std::set<String> partition_ids;
        for (const auto * bounds : {&old_bounds.at(key), &new_bounds.at(key)})
            for (const auto & [partition_id, _] : *bounds)
                partition_ids.insert(partition_id);
        for (const auto & partition_id : partition_ids)
        {
            Int64 old_bound = getBound(old_bounds.at(key), partition_id);
            Int64 new_bound = getBound(new_bounds.at(key), partition_id);
            if (old_bound > new_bound)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The old cursor is ahead of the new cursor in partition {} of table {}: {} > {}",
                    partition_id, key, old_bound, new_bound);
            Int64 committed_end = blocks.getCommittedEnd(partition_id, is_join ? 0 : old_bound);
            if (new_bound > committed_end)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "The cursors include block {} of partition {} of table {}, which this server has not committed",
                    committed_end, partition_id, key);
        }
    }
}

}

namespace DeltaQueryInternal
{

void addBounds(ASTs & conjuncts, const String & qualifier, const std::map<String, Int64> * lower,
    const std::map<String, Int64> & upper)
{
    auto part_block = [&](const String & field)
    {
        return makeASTFunction("tupleElement",
            makeASTFunction("mergeTreePartInfo", makeColumn(qualifier, "_part")), make_intrusive<ASTLiteral>(field));
    };
    ASTs part_bounds{makeASTFunction("less", part_block("min_block"), makeBound(qualifier, upper))};
    conjuncts.push_back(makeASTFunction("less", makeColumn(qualifier, "_block_number"), makeBound(qualifier, upper)));
    if (lower)
    {
        conjuncts.push_back(makeASTFunction("greaterOrEquals", makeColumn(qualifier, "_block_number"), makeBound(qualifier, *lower)));
        part_bounds.push_back(makeASTFunction("greaterOrEquals", part_block("max_block"), makeBound(qualifier, *lower)));
    }
    conjuncts.push_back(makeASTFunction("indexHint", std::move(part_bounds)));
}

}

/// The terms of one delta, see `buildDelta`.
struct DeltaQuery::DeltaTerms
{
    /// The rows of an input that a term reads: the rows below old, below new, and in [old, new).
    enum class Rows : uint8_t { Old, New, Delta };

    const DeltaQuery & query;
    const std::vector<Input> & inputs;
    const Cursor & old_bounds;
    const Cursor & new_bounds;
    const ASTSelectQuery & select;
    ASTPtr terms = make_intrusive<ASTExpressionList>();

    DeltaTerms(const DeltaQuery & query_, const Cursor & old_bounds_, const Cursor & new_bounds_)
        : query(query_)
        , inputs(query.inputs)
        , old_bounds(old_bounds_)
        , new_bounds(new_bounds_)
        , select(query.query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>())
    {}

    void addRows(ASTs & conjuncts, size_t j, Rows rows) const
    {
        const auto & key = inputs[j].key;
        addBounds(conjuncts, inputs[j].alias, rows == Rows::Delta ? &old_bounds.at(key) : nullptr,
            rows == Rows::Old ? old_bounds.at(key) : new_bounds.at(key));
    }

    /// Adds the term that reads rows `rows[j]` of each input j.
    void addTerm(const std::vector<Rows> & rows)
    {
        ASTs where;
        if (select.where())
            where.push_back(select.where()->clone());
        for (size_t j = 0; j < inputs.size(); ++j)
            addRows(where, j, rows[j]);
        auto term = select.clone();
        term->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
        terms->children.push_back(std::move(term));
    }

    /// Adds the term of the changes of input i, its rows in [old, new).
    void addChanges(size_t i)
    {
        std::vector<Rows> rows(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            rows[j] = j < i ? Rows::Old : Rows::New;
        rows[i] = Rows::Delta;
        addTerm(rows);
    }

    /// The UNION ALL of the terms.
    ASTPtr finish()
    {
        /// No input has new rows.
        if (terms->children.empty())
        {
            auto term = select.clone();
            term->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, make_intrusive<ASTLiteral>(Field(UInt8{0})));
            terms->children.push_back(std::move(term));
        }

        auto result = make_intrusive<ASTSelectWithUnionQuery>();
        result->union_mode = SelectUnionMode::UNION_ALL;
        result->is_normalized = true;
        result->list_of_selects = terms;
        result->children.push_back(terms);
        return result;
    }
};

/// The UNION ALL over i of Q, where input i reads its changes between old and new, inputs before it read the table as of
/// old, and inputs after it read the table as of new. That is the delta of a join of the inputs under the sequential update
/// rule.
ASTPtr DeltaQuery::buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    const auto tables = snapshotTables(inputs);
    const auto old_bounds = parseCursor(old_cursor, inputs, tables, "old cursor");
    const auto new_bounds = parseCursor(new_cursor, inputs, tables, "new cursor");
    checkCursors(tables, old_bounds, new_bounds, /*is_join=*/ inputs.size() > 1);

    const bool from_empty = std::ranges::all_of(inputs, [&](const auto & input) { return old_bounds.at(input.key).empty(); });
    DeltaTerms terms(*this, old_bounds, new_bounds);
    using Rows = DeltaTerms::Rows;
    /// From the empty cursor, the delta is the query as of the new cursor. One term reads it, instead of one term per input
    /// that each read all the other inputs.
    if (from_empty)
        terms.addTerm(std::vector<Rows>(inputs.size(), Rows::New));
    for (size_t i = 0; i < inputs.size() && !from_empty; ++i)
        if (tables.at(inputs[i].key).hasBlocks(old_bounds.at(inputs[i].key), new_bounds.at(inputs[i].key)))
            terms.addChanges(i);
    return terms.finish();
}

}
