#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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

/// The table of `input`.
ASTPtr makeTable(const Input & input)
{
    return make_intrusive<ASTTableIdentifier>(input.storage->getStorageID());
}

/// `(SELECT columns FROM table AS alias WHERE where)` over the table of `input`. The alias of the input lets `where` qualify
/// its columns like the conjuncts of the query.
ASTPtr makeSemiJoinSubquery(const Input & input, const Names & columns, ASTPtr where)
{
    auto select = makeSelect(makeIdentifiers(columns), withAlias(makeTable(input), input.alias), std::move(where));
    return makeSubquery(makeSelectWithUnion(std::move(select)));
}

/// The columns `names` of `input`, qualified by its alias.
ASTs makeColumns(const Input & input, const Names & names)
{
    ASTs columns;
    for (const auto & name : names)
        columns.push_back(makeColumn(input.alias, name));
    return columns;
}

/// `(k_1, ...) IN subquery` over the join keys of `input`.
ASTPtr makeJoinKeysIn(const Input & input, ASTPtr subquery)
{
    return makeASTFunction("in", makeTuple(makeColumns(input, input.join_keys)), std::move(subquery));
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

void setInput(ASTSelectQuery & select, const Input & input, ASTs conjuncts, ASTs & where, const String & sign_column)
{
    if (input.join_keys.empty())
    {
        std::ranges::move(conjuncts, std::back_inserter(where));
        return;
    }
    ASTs columns;
    for (const auto & column : input.columns)
        columns.push_back(withAlias(makeColumn(input.alias, column), column));
    ASTs group_by;
    ASTPtr having;
    if (input.is_signed)
    {
        group_by = makeColumns(input, input.columns);
        columns.push_back(withAlias(makeASTFunction("sum", makeColumn(input.alias, sign_column)), sign_column));
        having = makeASTFunction("notEquals", make_intrusive<ASTIdentifier>(sign_column), make_intrusive<ASTLiteral>(Field(UInt64{0})));
    }
    auto subquery = makeSubquery(makeSelect(std::move(columns), withAlias(makeTable(input), input.alias),
        makeConjunction(std::move(conjuncts)), std::move(group_by), std::move(having)));
    setTable(findAliasedTable(select, input.alias), withAlias(std::move(subquery), input.alias));
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
    /// Whether some terms come in pairs of opposite signs, see `consolidated_columns`.
    bool has_pairs = false;

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

    /// Adds the term that reads rows `rows[j]` of each input j, and of input `from` only those that also pass `own`. The
    /// rows of the term have weight `sign` times their weight in the query, and now() is `time`.
    void addTerm(const std::vector<Rows> & rows, std::optional<size_t> from, ASTs own, Int64 sign, std::optional<Int64> time)
    {
        std::vector<ASTs> conjuncts(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            addRows(conjuncts[j], j, rows[j]);
        if (from)
            std::ranges::move(own, std::back_inserter(conjuncts[*from]));

        auto term = select.clone();
        auto & term_select = term->as<ASTSelectQuery &>();
        ASTs where;
        if (select.where())
            where.push_back(select.where()->clone());
        for (size_t j = 0; j < inputs.size(); ++j)
            setInput(term_select, inputs[j], std::move(conjuncts[j]), where, query.sign_column);
        term_select.setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
        bindTerm(*term, sign, time);
        terms->children.push_back(std::move(term));
    }

    /// Adds the terms of the changes of input i, its rows in [old, new), at time `time`.
    void addChanges(size_t i, std::optional<Int64> time)
    {
        ASTs change;
        addRows(change, i, Rows::Delta);
        std::vector<Rows> rows(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            rows[j] = j < i && !inputs[j].lookup ? Rows::Old : Rows::New;
        if (inputs[i].join_keys.empty())
        {
            rows[i] = Rows::Delta;
            addTerm(rows, i, {}, 1, time);
            return;
        }
        auto keys = makeJoinKeysIn(inputs[i], makeSemiJoinSubquery(inputs[i], inputs[i].join_keys, makeConjunction(cloneAll(change))));
        for (auto [own_rows, sign] : {std::pair{Rows::New, Int64{1}}, std::pair{Rows::Old, Int64{-1}}})
        {
            rows[i] = own_rows;
            addTerm(rows, i, {keys->clone()}, sign, time);
        }
        has_pairs = true;
    }

    /// Adds the terms of the change of the time from `old_time` to `new_time`, see `clock_input`.
    void addClockChange(Int64 old_time, Int64 new_time)
    {
        std::vector<Rows> rows(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            rows[j] = inputs[j].lookup ? Rows::New : Rows::Old;
        auto filter_at = [&](Int64 time)
        {
            /// A conjunct with now() is a function of it, so `bindTerm` reaches every call.
            auto filter = makeConjunction(cloneAll(query.clock_conjuncts));
            bindTerm(*filter, {}, time);
            return filter;
        };
        auto old_filter = filter_at(old_time);
        auto new_filter = filter_at(new_time);
        auto changed = makeASTFunction("and", makeASTFunction("or", old_filter->clone(), new_filter->clone()),
            makeASTFunction("not", makeASTFunction("and", old_filter, new_filter)));
        for (auto [time, sign] : {std::pair{new_time, Int64{1}}, std::pair{old_time, Int64{-1}}})
            addTerm(rows, query.clock_input, {changed->clone()}, sign, time);
        has_pairs = true;
    }

    /// The UNION ALL of the terms, at time `time` if there are none.
    ASTPtr finish(std::optional<Int64> time)
    {
        /// No input has new rows.
        if (terms->children.empty())
        {
            auto term = select.clone();
            term->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, make_intrusive<ASTLiteral>(Field(UInt8{0})));
            bindTerm(*term, 1, time);
            terms->children.push_back(std::move(term));
        }

        auto result = make_intrusive<ASTSelectWithUnionQuery>();
        result->union_mode = SelectUnionMode::UNION_ALL;
        result->is_normalized = true;
        result->list_of_selects = terms;
        result->children.push_back(terms);
        if (!has_pairs || query.consolidated_columns.empty())
            return result;
        return consolidate(query.consolidated_columns, query.sign_column, {{makeSubquery(std::move(result)),
            makeIdentifiers(query.consolidated_columns), makeASTFunction("toInt64", make_intrusive<ASTIdentifier>(query.sign_column))}});
    }
};

/// The UNION ALL over i of Q, where input i reads its changes between old and new, inputs before it read the table as of
/// old, and inputs after it read the table as of new. That is the delta of a join of the inputs under the sequential update
/// rule.
///
/// A LEFT, SEMI, ANTI or ASOF join is linear in its left side only. A change of its right input i changes the result for
/// every left row of its join keys. So term i is the difference of two terms that read input i as of the new and as of the
/// old cursor, restricted to the join keys of the changes of input i.
///
/// A filter by now() is a join with the time, which is one more input after the others. Its term reads every input below
/// the old cursor, and it is the difference of the query at the new and at the old time over the rows whose filter changed.
ASTPtr DeltaQuery::buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    const auto tables = snapshotTables(inputs);
    const auto old_bounds = parseCursor(old_cursor, inputs, tables, "old cursor");
    const auto new_bounds = parseCursor(new_cursor, inputs, tables, "new cursor");
    checkCursors(tables, old_bounds, new_bounds, /*is_join=*/ inputs.size() > 1);

    const auto old_time = getTime(old_cursor);
    const auto new_time = getTime(new_cursor);
    const bool from_empty = std::ranges::all_of(inputs, [&](const auto & input) { return old_bounds.at(input.key).empty(); });
    if (clock_input && (!new_time || (!from_empty && !old_time)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The cursors of a delta query that calls now() need the time of their snapshot, which deltaQueryFrontier adds");

    DeltaTerms terms(*this, old_bounds, new_bounds);
    using Rows = DeltaTerms::Rows;
    /// From the empty cursor, the delta is the query as of the new cursor. One term reads it, instead of one term per input
    /// that each read all the other inputs.
    if (from_empty)
        terms.addTerm(std::vector<Rows>(inputs.size(), Rows::New), {}, {}, 1, new_time);
    /// The changes of a lookup table join with nothing, and the other terms read it as of the new cursor.
    for (size_t i = 0; i < inputs.size() && !from_empty; ++i)
        if (!inputs[i].lookup && tables.at(inputs[i].key).hasBlocks(old_bounds.at(inputs[i].key), new_bounds.at(inputs[i].key)))
            terms.addChanges(i, new_time);
    if (clock_input && !from_empty && *old_time != *new_time)
        terms.addClockChange(*old_time, *new_time);
    return terms.finish(new_time);
}

}
