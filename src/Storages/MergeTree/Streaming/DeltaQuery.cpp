#include <Storages/MergeTree/Streaming/DeltaQuery.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Joins.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>
#include <Storages/StorageProxy.h>

#include <deque>
#include <map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_block_number_column;
}

/// One occurrence of a table in the FROM clause of the query.
struct DeltaQuery::Input
{
    /// The name that qualifies columns of this occurrence in the query: its alias, or the table name as written.
    std::vector<String> qualifier;
    /// The key of the table in cursors: its UUID.
    String key;
    std::shared_ptr<MergeTreeData> storage;
};

namespace
{

using Input = DeltaQuery::Input;

/// Table UUID -> partition ID -> first excluded block number.
using Cursor = std::map<String, std::map<String, Int64>>;

[[noreturn]] void throwUnsupported(std::string_view construct)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support {}", construct);
}

/// Aggregates whose results over disjoint inputs, including empty ones, merge into the result over their union.
/// That excludes finalized `min` and `max`, which return 0 for an empty input.
bool isMergeableAggregate(const String & name)
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance();
    if (auto outer = combinators.tryFindSuffix(name); outer && outer->getName() == "State")
        return true;

    String base = name;
    while (auto combinator = combinators.tryFindSuffix(base))
    {
        if (combinator->getName() != "If")
            return false;
        base.resize(base.size() - combinator->getName().size());
    }
    const String & canonical = AggregateFunctionFactory::instance().getCanonicalNameIfAny(base);
    return canonical == "sum" || canonical == "count";
}

/// Rejects subqueries, which could read tables that the cursors do not cover. A join query also rejects window functions,
/// whose result over the union of the terms differs from the union of their results.
void checkExpression(const IAST & ast, bool is_join)
{
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectWithUnionQuery>())
        throwUnsupported("subqueries");
    if (const auto * function = ast.as<ASTFunction>(); is_join && function && function->isWindowFunction())
        throwUnsupported("window functions in joins");
    for (const auto & child : ast.children)
        checkExpression(*child, is_join);
}

/// Checks that each result column of a join query is either a mergeable aggregate or free of aggregates. Resolution inlines
/// aliases, so `sum(v) AS s, s + 1` fails too. Optimizations are skipped because they move arithmetic out of aggregates,
/// which would make `sum(v * 2)` fail.
void checkResultColumns(const QueryNode & query)
{
    for (const auto & column : query.getProjection().getNodes())
    {
        if (const auto * function = column->as<FunctionNode>(); function && function->isAggregateFunction())
        {
            if (!isMergeableAggregate(function->getFunctionName()))
                throwUnsupported(fmt::format("aggregate function {} in joins, use its -State form instead", function->getFunctionName()));
        }
        else if (hasAggregateFunctionNodes(column))
            throwUnsupported("expressions over aggregates in joins, select each aggregate as its own column");
    }
}

/// Checks the clauses whose result over the union of the terms of a join query differs from the union of their results.
void checkJoinClauses(const ASTSelectQuery & select)
{
    if (select.distinct)
        throwUnsupported("DISTINCT in joins");
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube || select.group_by_with_grouping_sets)
        throwUnsupported("WITH TOTALS, ROLLUP, CUBE or GROUPING SETS in joins");
    if (select.having())
        throwUnsupported("HAVING in joins");
    if (select.orderBy() || select.limitLength() || select.limitBy() || select.limitOffset())
        throwUnsupported("ORDER BY, LIMIT or LIMIT BY in joins");
    if (select.window() || select.qualify())
        throwUnsupported("WINDOW or QUALIFY in joins");
}

const ASTSelectQuery & getSingleSelect(const ASTSelectWithUnionQuery & query)
{
    if (query.list_of_selects->children.size() != 1)
        throwUnsupported("UNION");
    const auto * select = query.list_of_selects->children[0]->as<ASTSelectQuery>();
    if (!select)
        throwUnsupported("nested queries");
    return *select;
}

/// The table occurrences of the FROM clause, in order. Throws unless each is a table with block numbers, and the joins
/// between them are inner joins, whose delta is the UNION ALL of per-occurrence terms.
std::vector<Input> collectInputs(const ASTSelectQuery & select, const ContextPtr & context)
{
    if (!select.tables())
        throwUnsupported("queries without tables");

    std::vector<Input> inputs;
    for (const auto & child : select.tables()->children)
    {
        const auto & element = child->as<ASTTablesInSelectQueryElement &>();
        if (element.array_join)
            continue;

        if (const auto * join = element.table_join ? element.table_join->as<ASTTableJoin>() : nullptr)
        {
            bool inner = join->kind == JoinKind::Inner || join->kind == JoinKind::Cross || join->kind == JoinKind::Comma;
            bool all = join->strictness == JoinStrictness::Unspecified || join->strictness == JoinStrictness::All;
            if (!inner || !all || join->locality == JoinLocality::Global)
                throwUnsupported("joins other than INNER, CROSS and comma joins");
        }

        const auto & table_expression = element.table_expression->as<ASTTableExpression &>();
        if (table_expression.final || table_expression.sample_size || table_expression.stream_settings)
            throwUnsupported("FINAL, SAMPLE or STREAM");
        const auto * identifier = table_expression.database_and_table_name ? table_expression.database_and_table_name->as<ASTTableIdentifier>() : nullptr;
        if (!identifier)
            throwUnsupported("table functions or subqueries in FROM");

        auto table = DatabaseCatalog::instance().getTable(context->resolveStorageID(identifier->getTableId()), context);
        auto storage = castStorage<MergeTreeData>(table, DeferredTable::Load);
        if (!storage)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries read only MergeTree tables, but {} has engine {}",
                table->getStorageID().getNameForLogs(), table->getName());
        if (storage->merging_params.mode != MergeTreeData::MergingParams::Ordinary)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, whose merges rewrite rows",
                storage->getStorageID().getNameForLogs());
        if (!(*storage->getSettings())[MergeTreeSetting::enable_block_number_column])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta queries require table {} to set enable_block_number_column = 1",
                storage->getStorageID().getNameForLogs());
        if (!storage->getStorageID().hasUUID())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries require table {} to have a UUID, as tables of Atomic databases do",
                storage->getStorageID().getNameForLogs());

        String alias = identifier->tryGetAlias();
        inputs.push_back({
            .qualifier = alias.empty() ? identifier->name_parts : std::vector<String>{alias},
            .key = toString(storage->getStorageID().uuid),
            .storage = std::move(storage)});
    }
    return inputs;
}

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
            parts[part->info.getPartitionId()].push_back(part->info);
        }
        /// Built after the parts snapshot, the promoters report a part committed in between as in flight.
        promoters = storage.buildPromoters();
    }

    const LocalPartsByPartition & getParts() const { return parts; }

    /// Whether this server has no parts of the partition and no parts in flight to it, as after the partition is dropped.
    bool isAbsent(const String & partition_id) const { return !parts.contains(partition_id) && !promoters.contains(partition_id); }

    /// The first block number from `start` on that this server has not committed to the partition.
    Int64 getCommittedEnd(const String & partition_id, Int64 start) const
    {
        auto partition_parts = parts.find(partition_id);
        auto promoter = promoters.find(partition_id);
        if (partition_parts == parts.end() || promoter == promoters.end())
            return start;
        return promoteBlockNumber(start - 1, partition_parts->second, promoter->second) + 1;
    }

private:
    LocalPartsByPartition parts;
    CursorPromotersMap promoters;
};

std::map<String, TableBlocks> snapshotTables(const std::vector<Input> & inputs)
{
    std::map<String, TableBlocks> tables;
    for (const auto & input : inputs)
        tables.try_emplace(input.key, *input.storage);
    return tables;
}

/// Incremental refreshable materialized views over one table used to store the `STREAM` cursor of the table, which has
/// the partitions at the top.
bool isStreamCursor(const CursorTreeNode & tree)
{
    return std::ranges::any_of(tree, [](const auto & entry)
    {
        const auto * node = std::get_if<CursorTreeNodePtr>(&entry.second);
        return node && (*node)->hasValue("block_number");
    });
}

/// Parses `tree` and removes the partitions that this server has no parts of and no parts in flight to. Reading such a
/// partition returns no rows below any bound, so removing it changes no result. It also lets the partition start over
/// if it gets rows again.
Cursor parseCursor(const CursorTreeNodePtr & tree, const std::vector<Input> & inputs, const std::map<String, TableBlocks> & tables,
    std::string_view name)
{
    Cursor cursor;
    for (const auto & input : inputs)
        cursor[input.key];
    if (!tree)
        return cursor;

    auto parse_partitions = [&](const String & key, const CursorTreeNode & partitions)
    {
        for (const auto & [partition_id, value] : partitions)
        {
            const auto * position = std::get_if<CursorTreeNodePtr>(&value);
            if (!position || !(*position)->hasValue("block_number"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The {} has no block number for partition {} of table {}", name, partition_id, key);
            if ((*position)->getValue("block_offset", -1) != -1)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The {} ends inside a block of partition {} of table {}", name, partition_id, key);
            if (!tables.at(key).isAbsent(partition_id))
                cursor[key][partition_id] = (*position)->getValue("block_number");
        }
    };

    if (inputs.size() == 1 && isStreamCursor(*tree))
    {
        parse_partitions(inputs[0].key, *tree);
        return cursor;
    }
    for (const auto & [key, value] : *tree)
    {
        const auto * partitions = std::get_if<CursorTreeNodePtr>(&value);
        if (!partitions || !cursor.contains(key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The {} has key {}, which is not the UUID of a table of the query", name, key);
        parse_partitions(key, **partitions);
    }
    return cursor;
}

CursorTreeNodePtr makeCursorTree(const Cursor & cursor)
{
    auto tree = std::make_shared<CursorTreeNode>();
    for (const auto & [key, partitions] : cursor)
        for (const auto & [partition_id, block_number] : partitions)
            tree->getSubtreeOrCreate(key)->getSubtreeOrCreate(partition_id)->setValue("block_number", block_number);
    return tree;
}

Int64 getBound(const std::map<String, Int64> & partitions, const String & partition_id)
{
    auto it = partitions.find(partition_id);
    return it == partitions.end() ? 0 : it->second;
}

ASTPtr makeColumn(const std::vector<String> & qualifier, const String & column)
{
    auto parts = qualifier;
    parts.push_back(column);
    return make_intrusive<ASTIdentifier>(std::move(parts));
}

/// `transform(_partition_id, ids, bounds, 0)`, the bound of the partition of the row. A missing partition has bound 0.
ASTPtr makeBound(const std::vector<String> & qualifier, const std::map<String, Int64> & partitions)
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

/// Conjuncts that keep the rows of an input with `lower` <= `_block_number` < `upper`, where a null `lower` keeps every row
/// below `upper`. The conjuncts on `mergeTreePartInfo(_part)` depend only on the part, so they skip the parts outside the
/// bounds before reading them.
void addBounds(ASTs & conjuncts, const std::vector<String> & qualifier, const std::map<String, Int64> * lower,
    const std::map<String, Int64> & upper)
{
    auto part_block = [&](const String & field)
    {
        return makeASTFunction("tupleElement",
            makeASTFunction("mergeTreePartInfo", makeColumn(qualifier, "_part")), make_intrusive<ASTLiteral>(field));
    };
    conjuncts.push_back(makeASTFunction("less", makeColumn(qualifier, "_block_number"), makeBound(qualifier, upper)));
    conjuncts.push_back(makeASTFunction("less", part_block("min_block"), makeBound(qualifier, upper)));
    if (lower)
    {
        conjuncts.push_back(makeASTFunction("greaterOrEquals", makeColumn(qualifier, "_block_number"), makeBound(qualifier, *lower)));
        conjuncts.push_back(makeASTFunction("greaterOrEquals", part_block("max_block"), makeBound(qualifier, *lower)));
    }
}

/// A column of one input, like `o.id`.
struct InputColumn
{
    size_t input;
    String column;
};

/// The aliases that expressions of Q define. Table aliases are not included.
void collectExpressionAliases(const IAST & ast, std::unordered_set<String> & aliases)
{
    if (ast.as<ASTTableExpression>())
        return;
    if (auto alias = ast.tryGetAlias(); !alias.empty())
        aliases.insert(alias);
    for (const auto & child : ast.children)
        collectExpressionAliases(*child, aliases);
}

/// The input column that an identifier like `o.id` in Q names. The analyzer binds `o.id` to an alias `o` or to a column
/// `o` or `o.id` of any input before it binds `o` as a table qualifier. It also accepts the table name of an input that
/// has an alias. So this returns nothing unless `o` is the qualifier of exactly one input and names no alias or column.
std::optional<InputColumn> findInputColumn(
    const IAST & ast, const std::vector<Input> & inputs, const std::unordered_set<String> & expression_aliases)
{
    const auto * identifier = ast.as<ASTIdentifier>();
    if (!identifier || identifier->name_parts.size() < 2 || expression_aliases.contains(identifier->name_parts[0]))
        return {};
    std::vector<String> qualifier(identifier->name_parts.begin(), identifier->name_parts.end() - 1);
    std::optional<InputColumn> result;
    for (size_t k = 0; k < inputs.size(); ++k)
    {
        auto metadata = inputs[k].storage->getInMemoryMetadataPtr(nullptr, false);
        if (metadata->getColumns().hasColumnOrNested(GetColumnsOptions::All, qualifier[0]))
            return {};
        auto table_id = inputs[k].storage->getStorageID();
        bool matches = qualifier == inputs[k].qualifier || qualifier == std::vector<String>{table_id.table_name}
            || qualifier == std::vector<String>{table_id.database_name, table_id.table_name};
        if (matches && (result || qualifier != inputs[k].qualifier))
            return {};
        if (matches)
            result = InputColumn{k, identifier->name_parts.back()};
    }
    return result;
}

/// The semi-join reduction rewrites `a.x = b.y` into `b.y IN (SELECT x FROM a ...)`. `IN` casts `b.y` to the type of
/// `x` and looks it up by its binary representation. That lookup can disagree with `=`. For example, `-0.0 = 0.0` is true
/// and `-0.0 IN (SELECT 0.0)` is false. So only integer and string columns of one type count as join keys.
bool isSemiJoinKeyPair(const InputColumn & a, const InputColumn & b, const std::vector<Input> & inputs)
{
    auto get_type = [&](const InputColumn & column) -> DataTypePtr
    {
        auto metadata = inputs[column.input].storage->getInMemoryMetadataPtr(nullptr, false);
        auto name_and_type = metadata->getColumns().tryGetColumn(GetColumnsOptions::AllPhysicalAndAliases, column.column);
        return name_and_type ? removeLowCardinalityAndNullable(name_and_type->type) : nullptr;
    };
    auto a_type = get_type(a);
    auto b_type = get_type(b);
    return a_type && b_type && a_type->equals(*b_type) && (a_type->isValueRepresentedByInteger() || isStringOrFixedString(a_type));
}

/// The top-level conjuncts of ON and WHERE that equate join keys of two different inputs, like `q.order_id = o.id`.
/// Queries with ARRAY JOIN have none, because ARRAY JOIN without an alias replaces a column by its elements.
std::vector<std::array<InputColumn, 2>> collectJoinEqualities(const ASTSelectQuery & select, const std::vector<Input> & inputs)
{
    ASTs conjuncts;
    splitConjunctionsAst(select.where(), conjuncts);
    for (const auto & child : select.tables()->children)
    {
        const auto & element = child->as<ASTTablesInSelectQueryElement &>();
        if (element.array_join)
            return {};
        if (const auto * join = element.table_join ? element.table_join->as<ASTTableJoin>() : nullptr)
            splitConjunctionsAst(join->on_expression, conjuncts);
    }
    std::unordered_set<String> expression_aliases;
    collectExpressionAliases(select, expression_aliases);

    std::vector<std::array<InputColumn, 2>> equalities;
    for (const auto & conjunct : conjuncts)
    {
        const auto * function = conjunct->as<ASTFunction>();
        if (!function || function->name != "equals" || !function->arguments || function->arguments->children.size() != 2)
            continue;
        auto left = findInputColumn(*function->arguments->children[0], inputs, expression_aliases);
        auto right = findInputColumn(*function->arguments->children[1], inputs, expression_aliases);
        if (left && right && left->input != right->input && isSemiJoinKeyPair(*left, *right, inputs))
            equalities.push_back({*left, *right});
    }
    return equalities;
}

/// `(SELECT column FROM input WHERE where)`
ASTPtr makeSemiJoinSubquery(const Input & input, const String & column, ASTPtr where)
{
    auto select = make_intrusive<ASTSelectQuery>();
    auto columns = make_intrusive<ASTExpressionList>();
    columns->children.push_back(make_intrusive<ASTIdentifier>(column));
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(columns));
    select->replaceDatabaseAndTable(input.storage->getStorageID());
    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));

    auto selects = make_intrusive<ASTExpressionList>();
    selects->children.push_back(std::move(select));
    auto query = make_intrusive<ASTSelectWithUnionQuery>();
    query->list_of_selects = selects;
    query->children.push_back(std::move(selects));
    return make_intrusive<ASTSubquery>(std::move(query));
}

}

DeltaQuery::DeltaQuery(const ASTPtr & query_, const ContextPtr & context)
    : query(query_->clone())
{
    const auto & select = getSingleSelect(query->as<ASTSelectWithUnionQuery &>());
    inputs = collectInputs(select, context);
    const bool is_join = inputs.size() > 1;
    checkExpression(select, is_join);
    if (is_join)
        checkJoinClauses(select);

    InterpreterSelectQueryAnalyzer interpreter(query, context, SelectQueryOptions().analyze().ignoreASTOptimizations());
    if (is_join)
        checkResultColumns(interpreter.getQueryTree()->as<QueryNode &>());
}

DeltaQuery::~DeltaQuery() = default;

CursorTreeNodePtr DeltaQuery::getFrontier(const CursorTreeNodePtr & from) const
{
    const auto tables = snapshotTables(inputs);
    auto frontier = parseCursor(from, inputs, tables, "cursor");
    for (const auto & [key, blocks] : tables)
    {
        auto & partitions = frontier.at(key);
        for (const auto & [partition_id, _] : blocks.getParts())
            partitions.try_emplace(partition_id, 0);
        for (auto & [partition_id, bound] : partitions)
            bound = blocks.getCommittedEnd(partition_id, bound);
        std::erase_if(partitions, [](const auto & partition) { return partition.second <= 0; });
    }
    return makeCursorTree(frontier);
}

/// The UNION ALL over i of Q, where input i reads rows in [old, new), inputs before it read rows below old, and inputs after
/// it read rows below new. That is the delta of a join of the inputs under the sequential update rule.
///
/// Term i also gets a semi-join reduction. The reduction makes the cost of term i follow the delta instead of the size of the
/// other inputs. A breadth-first search from input i follows the join equalities. Each input b reached from input a by
/// `a.x = b.y` gets the conjunct `b.y IN (SELECT x FROM a WHERE ...)`, where the subquery's WHERE holds the bounds and
/// reduction of a in term i. Every row of the term satisfies that conjunct, because its row of a satisfies the same bounds
/// and reduction and has `x = b.y`. So the conjunct removes only rows that the join removes. The conjunct only filters rows
/// of b and never changes how many times a row appears, so it stays correct for inputs whose rows carry negative weights.
ASTPtr DeltaQuery::buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    const auto tables = snapshotTables(inputs);
    const auto old_bounds = parseCursor(old_cursor, inputs, tables, "old cursor");
    const auto new_bounds = parseCursor(new_cursor, inputs, tables, "new cursor");

    /// Term i reads the blocks of input i in [old, new). In a join, the other terms read the blocks below old or new.
    const bool is_join = inputs.size() > 1;
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

    const auto & select = getSingleSelect(query->as<ASTSelectWithUnionQuery &>());
    const auto equalities = collectJoinEqualities(select, inputs);
    auto terms = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto add_bounds = [&](ASTs & conjuncts, size_t j, const std::vector<String> & qualifier)
        {
            const auto & key = inputs[j].key;
            if (j < i)
                addBounds(conjuncts, qualifier, nullptr, old_bounds.at(key));
            else if (j == i)
                addBounds(conjuncts, qualifier, &old_bounds.at(key), new_bounds.at(key));
            else
                addBounds(conjuncts, qualifier, nullptr, new_bounds.at(key));
        };

        ASTs conjuncts;
        if (auto where = select.where())
            conjuncts.push_back(where->clone());
        for (size_t j = 0; j < inputs.size(); ++j)
            add_bounds(conjuncts, j, inputs[j].qualifier);

        /// Each subquery reads one table and defines no aliases, so its unqualified columns name columns of that table.
        /// `reductions[j]` is the reduction of input j with unqualified columns, for the subqueries of later hops.
        std::vector<ASTPtr> reductions(inputs.size());
        std::vector<bool> reached(inputs.size());
        reached[i] = true;
        std::deque<size_t> queue{i};
        while (!queue.empty())
        {
            size_t from = queue.front();
            queue.pop_front();
            for (const auto & equality : equalities)
                for (size_t side : {0, 1})
                {
                    const auto & [a, x] = equality[side];
                    const auto & [b, y] = equality[1 - side];
                    if (a != from || reached[b])
                        continue;
                    reached[b] = true;
                    queue.push_back(b);

                    ASTs where;
                    add_bounds(where, a, {});
                    if (reductions[a])
                        where.push_back(reductions[a]->clone());
                    auto subquery = makeSemiJoinSubquery(inputs[a], x, makeASTFunction("and", std::move(where)));
                    reductions[b] = makeASTFunction("in", make_intrusive<ASTIdentifier>(y), subquery->clone());
                    conjuncts.push_back(makeASTFunction("in", makeColumn(inputs[b].qualifier, y), std::move(subquery)));
                }
        }

        auto term = select.clone();
        term->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", std::move(conjuncts)));
        terms->children.push_back(std::move(term));
    }

    auto result = make_intrusive<ASTSelectWithUnionQuery>();
    result->union_mode = SelectUnionMode::UNION_ALL;
    result->is_normalized = true;
    result->list_of_selects = terms;
    result->children.push_back(terms);
    return result;
}

}
