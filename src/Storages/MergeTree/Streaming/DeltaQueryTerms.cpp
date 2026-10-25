#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>

#include <deque>
#include <map>
#include <set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsBool optimize_and_compare_chain;
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

/// `map(partition_id, bound, ...)`.
ASTPtr makeBoundsMap(const std::map<String, Int64> & bounds)
{
    ASTs arguments;
    for (const auto & [partition_id, bound] : bounds)
    {
        arguments.push_back(make_intrusive<ASTLiteral>(Field(partition_id)));
        arguments.push_back(make_intrusive<ASTLiteral>(Field(bound)));
    }
    return makeASTFunction("map", std::move(arguments));
}

/// `(alias._block_number, alias._block_offset) IN (SELECT _block_number, _block_offset FROM deltaQueryPatches(...))`, which
/// keeps the rows of `input` that its lightweight updates with versions in [`lower`, `upper`) change.
ASTPtr makeUpdatedRowsIn(const Input & input, const std::map<String, Int64> & lower, const std::map<String, Int64> & upper)
{
    const auto table_id = input.storage->getStorageID();
    auto patches = makeASTFunction("deltaQueryPatches", make_intrusive<ASTLiteral>(Field(table_id.database_name)),
        make_intrusive<ASTLiteral>(Field(table_id.table_name)), makeBoundsMap(lower), makeBoundsMap(upper));
    return makeASTFunction("in",
        makeASTFunction("tuple", makeColumn(input.alias, "_block_number"), makeColumn(input.alias, "_block_offset")),
        makeSubquery(makeSelect(makeIdentifiers({"_block_number", "_block_offset"}), std::move(patches))));
}

/// `(SELECT columns FROM table AS alias WHERE where UNION ALL ...)` over the pairs `sources` of a table of `input` and a
/// filter. The alias of the input lets the filters qualify its columns like the conjuncts of the query. With a non-empty
/// `default_type`, the subquery also returns the default of that type.
ASTPtr makeSemiJoinSubquery(const Input & input, const Names & columns, const std::vector<std::pair<ASTPtr, ASTPtr>> & sources,
    const String & default_type = {})
{
    ASTs selects;
    for (const auto & [table, where] : sources)
        selects.push_back(makeSelect(makeIdentifiers(columns), withAlias(table->clone(), input.alias), where->clone()));
    if (!default_type.empty())
        selects.push_back(makeSelect({makeASTFunction("defaultValueOfTypeName", make_intrusive<ASTLiteral>(Field(default_type)))}, nullptr));
    return makeSubquery(makeUnionAll(selects));
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
/// the blocks of input i in [old, new). In a join, the other terms read the blocks below old or new, and the updates of an
/// input read its rows below old.
void checkCursors(const std::map<String, TableBlocks> & tables, const Cursor & old_bounds, const Cursor & new_bounds, bool is_join)
{
    for (const auto & [key, blocks] : tables)
    {
        std::set<String> partition_ids;
        for (const auto * bounds : {&old_bounds.at(key), &new_bounds.at(key)})
            for (const auto & [partition_id, _] : *bounds)
                partition_ids.insert(partition_id);
        const bool reads_old_rows = is_join || blocks.hasBlocks(old_bounds.at(key), new_bounds.at(key), /*updates=*/ true);
        for (const auto & partition_id : partition_ids)
        {
            Int64 old_bound = getBound(old_bounds.at(key), partition_id);
            Int64 new_bound = getBound(new_bounds.at(key), partition_id);
            if (old_bound > new_bound)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The old cursor is ahead of the new cursor in partition {} of table {}: {} > {}",
                    partition_id, key, old_bound, new_bound);
            Int64 committed_end = blocks.getCommittedEnd(partition_id, reads_old_rows ? 0 : old_bound);
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

ASTPtr makeSnapshot(const Input & input, const std::map<String, Int64> & bounds)
{
    const auto table_id = input.storage->getStorageID();
    if (!input.updatable)
        return make_intrusive<ASTTableIdentifier>(table_id);
    return makeASTFunction("deltaQuerySnapshot", make_intrusive<ASTLiteral>(Field(table_id.database_name)),
        make_intrusive<ASTLiteral>(Field(table_id.table_name)), makeBoundsMap(bounds));
}

void setInput(ASTSelectQuery & select, const Input & input, ASTPtr table, ASTs conjuncts, ASTs & where, const String & sign_column)
{
    if (input.join_keys.empty())
    {
        table->setAlias(input.alias);
        setTable(findAliasedTable(select, input.alias), std::move(table));
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
    auto subquery = makeSubquery(makeSelect(std::move(columns), withAlias(std::move(table), input.alias),
        makeConjunction(std::move(conjuncts)), std::move(group_by), std::move(having)));
    setTable(findAliasedTable(select, input.alias), withAlias(std::move(subquery), input.alias));
}

}

bool DeltaQuery::canCorrect() const
{
    return !consolidated_columns.empty() || !summed_columns.empty();
}

ASTPtr DeltaQuery::buildCorrection(const CursorTreeNodePtr & new_cursor, const StorageID & target) const
{
    chassert(canCorrect());
    auto query_rows = makeSubquery(buildDelta(nullptr, new_cursor));
    auto target_table = make_intrusive<ASTTableIdentifier>(target);
    if (!consolidated_columns.empty())
    {
        auto weight = [&] { return makeASTFunction("toInt64", make_intrusive<ASTIdentifier>(sign_column)); };
        return consolidate(consolidated_columns, sign_column, {
            {std::move(query_rows), makeIdentifiers(consolidated_columns), weight()},
            {std::move(target_table), castToResultTypes(consolidated_columns), makeASTFunction("negate", weight())}});
    }
    /// Each group gets one row with the sums of the query minus the sums of the target. A target column can have an unsigned
    /// type that holds negative sums modulo its range. The products with a signed factor have one type in both selects.
    ASTs columns;
    ASTs retracted;
    ASTs results;
    Names keys;
    ASTs nonzero;
    const auto types = getResultColumns();
    for (const auto & [name, is_sum] : summed_columns)
    {
        auto column = [&] { return make_intrusive<ASTIdentifier>(name); };
        auto times = [&](Int64 factor)
        {
            return makeASTFunction("multiply", column(), makeASTFunction("toInt8", make_intrusive<ASTLiteral>(Field(factor))));
        };
        if (is_sum)
        {
            columns.push_back(withAlias(times(1), name));
            retracted.push_back(times(-1));
            results.push_back(withAlias(makeASTFunction("sum", column()), name));
            nonzero.push_back(makeASTFunction("notEquals", column(), make_intrusive<ASTLiteral>(Field(UInt64{0}))));
        }
        else
        {
            columns.push_back(column());
            retracted.push_back(castTo(name, types));
            results.push_back(column());
            keys.push_back(name);
        }
    }
    auto rows = makeUnionAll({makeSelect(std::move(columns), std::move(query_rows)), makeSelect(std::move(retracted), std::move(target_table))});
    auto changed = nonzero.size() == 1 ? std::move(nonzero[0]) : makeASTFunction("or", std::move(nonzero));
    return makeSelectWithUnion(makeSelect(std::move(results), makeSubquery(std::move(rows)), nullptr, makeIdentifiers(keys), std::move(changed)));
}

/// The terms of one delta, see `buildDelta`.
struct DeltaQuery::DeltaTerms
{
    /// The rows of an input that a term reads: the rows below old as of old, below new as of new, in [old, new) as of new, and
    /// below old as of new.
    enum class Rows : uint8_t { Old, New, Delta, Prior };
    /// The rows of an input that also pass `filters`.
    struct Selection
    {
        Rows rows;
        ASTs filters;
    };

    const DeltaQuery & query;
    const std::vector<Input> & inputs;
    const Cursor & old_bounds;
    const Cursor & new_bounds;
    const ASTSelectQuery & select;
    ASTPtr terms = make_intrusive<ASTExpressionList>();
    /// Whether some terms come in pairs of opposite signs, see `consolidated_columns`.
    bool has_pairs = false;
    /// For a query with aggregates, the aggregation over the union of the terms, and the columns that each term returns for it.
    ASTPtr outer;
    ASTs term_columns;

    /// The delta of a query with aggregates is the sum of the aggregates over the rows of all its terms. So each term returns
    /// the identifiers that the aggregates and the keys read, and one aggregation over the union of the terms computes the
    /// delta. Planning then builds the aggregates once instead of once per term.
    DeltaTerms(const DeltaQuery & query_, const Cursor & old_bounds_, const Cursor & new_bounds_)
        : query(query_)
        , inputs(query.inputs)
        , old_bounds(old_bounds_)
        , new_bounds(new_bounds_)
        , select(query.query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>())
    {
        if (!select.groupBy() && !std::ranges::any_of(select.select()->children, [](const auto & column) { return isAggregate(*column); }))
            return;
        outer = select.clone();
        auto & outer_select = outer->as<ASTSelectQuery &>();
        outer_select.setExpression(ASTSelectQuery::Expression::WHERE, nullptr);
        outer_select.setExpression(ASTSelectQuery::Expression::PREWHERE, nullptr);
        /// The columns of the union need names that no alias or identifier of the query has, or the outer query would read
        /// the wrong columns.
        NameSet taken;
        collectNames(select, taken);
        std::map<String, String> names;
        auto rename = [&](const ASTPtr & node) -> ASTPtr
        {
            if (node->as<ASTSubquery>())
                return node->clone();
            const auto * identifier = node->as<ASTIdentifier>();
            if (!identifier)
                return nullptr;
            auto [it, inserted] = names.try_emplace(identifier->name());
            if (inserted)
            {
                it->second = makeUniqueName("__delta_column", taken);
                taken.insert(it->second);
                term_columns.push_back(withAlias(node->clone(), it->second));
            }
            auto renamed = make_intrusive<ASTIdentifier>(it->second);
            renamed->setAlias(node->tryGetAlias());
            return renamed;
        };
        std::multiset<String> lambda_parameters;
        for (const auto & list : {outer_select.select(), outer_select.groupBy()})
            if (list)
                for (auto & child : list->children)
                    visitExpression(child, *list, rename, lambda_parameters);
    }

    /// Makes `term` return the columns that the aggregation reads.
    void setTermColumns(ASTSelectQuery & term) const
    {
        if (!outer)
            return;
        auto columns = make_intrusive<ASTExpressionList>();
        columns->children = cloneAll(term_columns);
        if (columns->children.empty())
            columns->children.push_back(make_intrusive<ASTLiteral>(Field(UInt8{1})));
        term.setExpression(ASTSelectQuery::Expression::SELECT, std::move(columns));
        term.setExpression(ASTSelectQuery::Expression::GROUP_BY, nullptr);
    }

    void addRows(ASTs & conjuncts, size_t j, Rows rows) const
    {
        const auto & key = inputs[j].key;
        addBounds(conjuncts, inputs[j].alias, rows == Rows::Delta ? &old_bounds.at(key) : nullptr,
            rows == Rows::Old || rows == Rows::Prior ? old_bounds.at(key) : new_bounds.at(key));
    }

    /// The table of input j as of the cursor of `rows`.
    ASTPtr tableAt(size_t j, Rows rows) const
    {
        const auto & key = inputs[j].key;
        return makeSnapshot(inputs[j], rows == Rows::Old ? old_bounds.at(key) : new_bounds.at(key));
    }

    /// The table and the filter of each selection of input j.
    std::vector<std::pair<ASTPtr, ASTPtr>> getSources(size_t j, const std::vector<Selection> & selections) const
    {
        std::vector<std::pair<ASTPtr, ASTPtr>> sources;
        for (const auto & [rows, filters] : selections)
        {
            ASTs conjuncts;
            addRows(conjuncts, j, rows);
            for (const auto & filter : filters)
                conjuncts.push_back(filter->clone());
            sources.emplace_back(tableAt(j, rows), makeConjunction(std::move(conjuncts)));
        }
        return sources;
    }

    /// The conjuncts of each input that narrow the ASOF joins in a term that reads rows `rows[j]` of each input j, after the
    /// change `change` of input `from`, see `AsofJoin`. For a backward join, a change at time s changes only the matches of
    /// the left rows from s on. The match of such a row is at or after the latest right row of its keys at or before s. So
    /// the right input reads the rows of a key from s minus the lookback on if the key has a row from there to s, and all
    /// the rows of the key otherwise. Here s is the earliest time of the change. A forward join mirrors that.
    std::vector<ASTs> narrowAsof(const std::vector<Rows> & rows, size_t from, const std::vector<Selection> & change) const
    {
        std::vector<ASTs> extra(inputs.size());
        for (const auto & asof : query.asof_joins)
        {
            if (from != asof.left && from != asof.right)
                continue;
            const auto & left = inputs[asof.left];
            const auto & right = inputs[asof.right];
            const auto changes = getSources(from, change);
            const auto & time = from == asof.left ? asof.left_time : asof.right_time;
            const auto & keys = from == asof.left ? asof.left_keys : asof.right_keys;
            /// The comparisons `t >= b` and `t < s` or `t <= s` of a backward join. A forward join mirrors them.
            const auto * after = asof.backward ? "greaterOrEquals" : "lessOrEquals";
            const auto * before = asof.backward ? (asof.strict ? "less" : "lessOrEquals") : (asof.strict ? "greater" : "greaterOrEquals");
            auto start = [&]
            {
                return makeSubquery(makeSelect({makeASTFunction(asof.backward ? "min" : "max", make_intrusive<ASTIdentifier>(time))},
                    makeSemiJoinSubquery(inputs[from], {time}, changes)));
            };
            auto limit = [&] { return makeASTFunction(asof.backward ? "minus" : "plus", start(), asof.lookback->clone()); };
            auto right_keys = [&] { return makeTuple(makeColumns(right, asof.right_keys)); };
            auto right_time = [&] { return makeColumn(right.alias, asof.right_time); };
            auto touched = [&] { return makeSemiJoinSubquery(inputs[from], keys, changes); };
            /// The touched keys that have a right row from the limit to s, in the rows of the right input in the term.
            auto [table, where] = getSources(asof.right, {{rows[asof.right], {}}}).at(0);
            auto near = makeSelect({right_keys()}, withAlias(std::move(table), right.alias), makeConjunction({std::move(where),
                makeASTFunction(after, right_time(), limit()), makeASTFunction(before, right_time(), start()),
                makeASTFunction("in", right_keys(), touched())}));
            auto far = makeSelect(makeIdentifiers(keys), touched(),
                makeASTFunction("notIn", makeTuple(makeIdentifiers(keys)), makeSubquery(std::move(near))));
            extra[asof.right].push_back(makeASTFunction("or", makeASTFunction(after, right_time(), limit()),
                makeASTFunction("in", right_keys(), makeSubquery(std::move(far)))));
            if (from == asof.right)
                extra[asof.left].push_back(makeASTFunction(after, makeColumn(left.alias, asof.left_time), start()));
        }
        return extra;
    }

    /// Adds the term that reads rows `rows[j]` of each input j, and of input `from` only those that also pass `own`. The
    /// reduction starts from the selections `change` of input `from`, or without them from its rows in the term. The rows of
    /// the term have weight `sign` times their weight in the query, and now() is `time`. Input j also gets `extra[j]`.
    void addTerm(const std::vector<Rows> & rows, std::optional<size_t> from, ASTs own, const std::vector<Selection> & change,
        Int64 sign, std::optional<Int64> time, std::vector<ASTs> extra = {})
    {
        std::vector<ASTs> conjuncts(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            addRows(conjuncts[j], j, rows[j]);
        if (from)
            std::ranges::move(own, std::back_inserter(conjuncts[*from]));
        for (size_t j = 0; j < extra.size(); ++j)
            std::ranges::move(extra[j], std::back_inserter(conjuncts[j]));

        std::vector<bool> reached(inputs.size());
        /// The subquery of the conjunct `column IN subquery` that the reduction gave each input, by column.
        std::vector<std::map<String, ASTPtr>> reducing_sets(inputs.size());
        std::deque<size_t> queue;
        if (from)
        {
            reached[*from] = true;
            queue.push_back(*from);
        }
        while (!queue.empty())
        {
            size_t a = queue.front();
            queue.pop_front();
            for (const auto & edge : query.semi_join_edges)
            {
                /// A left row without a match joins whatever the rows of the right side. So only the terms of the changes of
                /// the right side reduce the left side by them. There the left rows without a match in the changes cancel.
                if (edge.from != a || reached[edge.to] || (edge.to_left_side && a != *from))
                    continue;
                reached[edge.to] = true;
                queue.push_back(edge.to);
                /// The defaults that pad a left row without a match can match rows of the other side of the equality.
                const bool padded = !inputs[a].join_keys.empty() && !edge.to_left_side;
                ASTPtr subquery;
                if (auto it = reducing_sets[a].find(edge.from_column); a != *from && !padded && it != reducing_sets[a].end())
                    subquery = it->second->clone();
                else
                {
                    auto sources = a == *from && !change.empty() ? getSources(a, change)
                        : std::vector<std::pair<ASTPtr, ASTPtr>>{{tableAt(a, rows[a]), makeConjunction(cloneAll(conjuncts[a]))}};
                    subquery = makeSemiJoinSubquery(inputs[a], {edge.from_column}, sources, padded ? edge.from_type : "");
                }
                reducing_sets[edge.to].try_emplace(edge.to_column, subquery);
                conjuncts[edge.to].push_back(makeASTFunction("in", makeColumn(inputs[edge.to].alias, edge.to_column), subquery->clone()));
            }
        }

        auto term = select.clone();
        auto & term_select = term->as<ASTSelectQuery &>();
        ASTs where;
        if (select.where())
            where.push_back(select.where()->clone());
        for (size_t j = 0; j < inputs.size(); ++j)
            setInput(term_select, inputs[j], tableAt(j, rows[j]), std::move(conjuncts[j]), where, query.sign_column);
        term_select.setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
        /// The reduction already filters each input by the keys that can join. A runtime filter of a join would probe every
        /// row read a second time.
        if (from)
            setSetting(term_select, "enable_join_runtime_filters", false);
        setTermColumns(term_select);
        bindTerm(*term, sign, time);
        terms->children.push_back(std::move(term));
    }

    /// Adds the terms of the changes of input i, whose blocks are `blocks`, at time `time`.
    void addChanges(size_t i, const TableBlocks & blocks, std::optional<Int64> time)
    {
        const auto & lower = old_bounds.at(inputs[i].key);
        const auto & upper = new_bounds.at(inputs[i].key);
        const bool updated = blocks.hasBlocks(lower, upper, /*updates=*/ true);
        /// The changes of input i. The rows that the updates change retract as of old and insert as of new.
        std::vector<Selection> changes;
        if (blocks.hasBlocks(lower, upper, /*updates=*/ false))
            changes.push_back({Rows::Delta, {}});
        if (updated)
        {
            auto updated_rows = makeUpdatedRowsIn(inputs[i], lower, upper);
            changes.push_back({Rows::Prior, {updated_rows->clone()}});
            changes.push_back({Rows::Old, {updated_rows}});
        }
        if (changes.empty())
            return;
        std::vector<Rows> rows(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            rows[j] = j < i && !inputs[j].lookup ? Rows::Old : Rows::New;
        if (inputs[i].join_keys.empty())
        {
            for (const auto & [own_rows, filters] : changes)
            {
                rows[i] = own_rows;
                addTerm(rows, i, cloneAll(filters), {}, own_rows == Rows::Old ? -1 : 1, time,
                    narrowAsof(rows, i, {{own_rows, cloneAll(filters)}}));
            }
            has_pairs |= updated;
            return;
        }
        auto keys = makeJoinKeysIn(inputs[i], makeSemiJoinSubquery(inputs[i], inputs[i].join_keys, getSources(i, changes)));
        for (auto [own_rows, sign] : {std::pair{Rows::New, Int64{1}}, std::pair{Rows::Old, Int64{-1}}})
        {
            rows[i] = own_rows;
            addTerm(rows, i, {keys->clone()}, changes, sign, time, narrowAsof(rows, i, changes));
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
            addTerm(rows, query.clock_input, {changed->clone()}, {}, sign, time);
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
            setTermColumns(term->as<ASTSelectQuery &>());
            bindTerm(*term, 1, time);
            terms->children.push_back(std::move(term));
        }

        auto result = make_intrusive<ASTSelectWithUnionQuery>();
        result->union_mode = SelectUnionMode::UNION_ALL;
        result->is_normalized = true;
        result->list_of_selects = terms;
        result->children.push_back(terms);
        if (outer)
        {
            /// The columns of the outer query come from the union of the terms, and hashing an expression over them hashes the
            /// union. The optimization of comparison chains would hash the terms for each comparison in the aggregates, and the
            /// outer query has no filter for it to improve. The terms keep the setting of the query.
            const bool compare_chain = query.analysis_context->getSettingsRef()[Setting::optimize_and_compare_chain];
            for (auto & term : terms->children)
                setSetting(term->as<ASTSelectQuery &>(), "optimize_and_compare_chain", compare_chain);
            setSetting(outer->as<ASTSelectQuery &>(), "optimize_and_compare_chain", false);
            outer->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::TABLES, makeTables(makeSubquery(std::move(result))));
            return makeSelectWithUnion(std::move(outer));
        }
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
/// The changes of input i are its rows in [old, new) as of new, and the rows below old that its lightweight updates in
/// [old, new) change. Those rows retract as of old and insert as of new. A term reads a table as of a cursor through
/// `deltaQuerySnapshot`, which applies the updates below the cursor, and the bounds of `addBounds`, which keep the rows below
/// it. `deltaQueryPatches` names the rows that the updates change.
///
/// Term i also gets a semi-join reduction. The reduction makes the cost of term i follow the delta instead of the size of the
/// other inputs. A breadth-first search from input i follows the join equalities. Each input b reached from input a by
/// `a.x = b.y` gets the conjunct `b.y IN (SELECT x FROM a WHERE ...)`, where the subquery's WHERE holds the bounds and
/// reduction of a in term i. Every row of the term satisfies that conjunct, because its row of a satisfies the same bounds
/// and reduction and has `x = b.y`. So the conjunct removes only rows that the join removes. The conjunct only filters rows
/// of b and never changes how many times a row appears, so it stays correct for inputs whose rows carry negative weights.
/// If the reduction already gave a the conjunct `a.x IN s`, then b gets `b.y IN s` instead. That set doesn't remove the keys
/// that a lacks, but building it doesn't read a, and every input with a column equal to `x` probes the same set.
///
/// A LEFT, SEMI, ANTI or ASOF join is linear in its left side only. A change of its right input i changes the result for
/// every left row of its join keys. So term i is the difference of two terms that read input i as of the new and as of the
/// old cursor, restricted to the join keys of the changes of input i. The reduction starts from those changes.
///
/// A filter by now() is a join with the time, which is one more input after the others. Its term reads every input below
/// the old cursor, and it is the difference of the query at the new and at the old time over the rows whose filter changed.
ASTPtr DeltaQuery::buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    const auto tables = snapshotTables(inputs);
    const auto old_bounds = parseCursor(old_cursor, inputs, tables, "old cursor");
    const auto new_bounds = parseCursor(new_cursor, inputs, tables, "new cursor");
    checkCursors(tables, old_bounds, new_bounds, /*is_join=*/ inputs.size() > 1);
    /// The terms read a lookup table only as of the new cursor.
    for (const auto & input : inputs)
    {
        if (!input.lookup)
            tables.at(input.key).checkReadableAsOf(old_bounds.at(input.key));
        tables.at(input.key).checkReadableAsOf(new_bounds.at(input.key));
    }

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
        terms.addTerm(std::vector<Rows>(inputs.size(), Rows::New), {}, {}, {}, 1, new_time);
    /// The changes of a lookup table join with nothing, and the other terms read it as of the new cursor.
    for (size_t i = 0; i < inputs.size() && !from_empty; ++i)
        if (!inputs[i].lookup)
            terms.addChanges(i, tables.at(inputs[i].key), new_time);
    if (clock_input && !from_empty && *old_time != *new_time)
        terms.addClockChange(*old_time, *new_time);
    return terms.finish(new_time);
}

}
