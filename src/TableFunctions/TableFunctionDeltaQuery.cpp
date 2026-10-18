#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Joins.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageValues.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

#include <deque>
#include <map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_block_number_column;
}

namespace
{

/// Table key ('db.table') -> partition id -> highest block number included.
using DeltaCursor = std::map<String, std::map<String, UInt64>>;

constexpr auto cursor_type_name = "Map(String, Map(String, UInt64))";

/// One occurrence of a table in the FROM clause of the delta query.
struct DeltaInput
{
    /// The name that qualifies columns of this occurrence in the query: its alias, or the table name as written.
    std::vector<String> qualifier;
    String key;
    std::shared_ptr<StorageMergeTree> storage;
};

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

void checkExpression(const IAST & ast)
{
    if (ast.as<ASTSubquery>() || ast.as<ASTSelectWithUnionQuery>())
        throwUnsupported("subqueries");
    if (const auto * function = ast.as<ASTFunction>(); function && function->isWindowFunction())
        throwUnsupported("window functions");
    for (const auto & child : ast.children)
        checkExpression(*child);
}

/// Resolves Q, which checks the caller's grants on its tables as for any `SELECT`, and checks that each result column is
/// either a mergeable aggregate or free of aggregates. Resolution inlines aliases, so `sum(v) AS s, s + 1` fails too.
/// Optimizations are skipped because they move arithmetic out of aggregates, which would make `sum(v * 2)` fail.
void checkResultColumns(const ASTPtr & query, const ContextPtr & context)
{
    InterpreterSelectQueryAnalyzer interpreter(query, context, SelectQueryOptions().analyze().ignoreASTOptimizations());
    for (const auto & column : interpreter.getQueryTree()->as<QueryNode &>().getProjection().getNodes())
    {
        if (const auto * function = column->as<FunctionNode>(); function && function->isAggregateFunction())
        {
            if (!isMergeableAggregate(function->getFunctionName()))
                throwUnsupported(fmt::format("aggregate function {}, use its -State form instead", function->getFunctionName()));
        }
        else if (hasAggregateFunctionNodes(column))
            throwUnsupported("expressions over aggregates, select each aggregate as its own column");
    }
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

/// Checks that Q has a shape whose delta is the UNION ALL of per-input terms, and returns its table occurrences in FROM order.
std::vector<DeltaInput> collectDeltaInputs(const ASTPtr & query, const ContextPtr & context)
{
    const auto & select = getSingleSelect(query->as<ASTSelectWithUnionQuery &>());

    if (select.distinct)
        throwUnsupported("DISTINCT");
    if (select.group_by_with_totals || select.group_by_with_rollup || select.group_by_with_cube || select.group_by_with_grouping_sets)
        throwUnsupported("WITH TOTALS, ROLLUP, CUBE or GROUPING SETS");
    if (select.having())
        throwUnsupported("HAVING");
    if (select.orderBy() || select.limitLength() || select.limitBy() || select.limitOffset())
        throwUnsupported("ORDER BY, LIMIT or LIMIT BY");
    if (select.window() || select.qualify())
        throwUnsupported("WINDOW or QUALIFY");
    if (!select.tables())
        throwUnsupported("queries without tables");

    std::vector<DeltaInput> inputs;
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
        auto storage = castStorage<StorageMergeTree>(table, DeferredTable::Load);
        if (!storage)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries read only plain MergeTree tables, but {} has engine {}",
                table->getStorageID().getNameForLogs(), table->getName());
        if (storage->merging_params.mode != MergeTreeData::MergingParams::Ordinary)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, whose merges rewrite rows",
                storage->getStorageID().getNameForLogs());
        if (!(*storage->getSettings())[MergeTreeSetting::enable_block_number_column])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta queries require table {} to set enable_block_number_column = 1",
                storage->getStorageID().getNameForLogs());

        String alias = identifier->tryGetAlias();
        inputs.push_back({
            .qualifier = alias.empty() ? identifier->name_parts : std::vector<String>{alias},
            .key = storage->getStorageID().getFullTableName(),
            .storage = std::move(storage)});
    }

    checkExpression(*select.select());
    for (auto expression : {select.prewhere(), select.where(), select.groupBy(), select.with()})
        if (expression)
            checkExpression(*expression);
    for (const auto & child : select.tables()->children)
    {
        const auto & element = child->as<ASTTablesInSelectQueryElement &>();
        if (element.array_join)
            checkExpression(*element.array_join);
        if (const auto * join = element.table_join ? element.table_join->as<ASTTableJoin>() : nullptr)
            if (join->on_expression)
                checkExpression(*join->on_expression);
    }
    checkResultColumns(query, context);

    return inputs;
}

/// The highest committed block number of every partition of every input.
/// `MergeTreeSink` allocates a block number and commits its part under one lock, so a plain `MergeTree` has no
/// uncommitted blocks below this bound.
DeltaCursor computeFrontier(const std::vector<DeltaInput> & inputs)
{
    DeltaCursor frontier;
    for (const auto & input : inputs)
    {
        if (!input.storage->getPatchPartsVectorForInternalUsage().empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, which has lightweight updates",
                input.storage->getStorageID().getNameForLogs());

        auto & partitions = frontier[input.key];
        for (const auto & part : input.storage->getDataPartsVectorForInternalUsage())
        {
            if (part->info.mutation != 0)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, which has mutated part {}",
                    input.storage->getStorageID().getNameForLogs(), part->name);
            auto & bound = partitions[part->info.getPartitionId()];
            bound = std::max<UInt64>(bound, part->info.max_block);
        }
    }
    return frontier;
}

DataTypePtr getCursorType()
{
    return DataTypeFactory::instance().get(cursor_type_name);
}

Field cursorToField(const DeltaCursor & cursor)
{
    Map tables;
    for (const auto & [key, partitions] : cursor)
    {
        Map bounds;
        for (const auto & [partition_id, bound] : partitions)
            bounds.push_back(Tuple{partition_id, bound});
        tables.push_back(Tuple{key, std::move(bounds)});
    }
    return tables;
}

DeltaCursor parseCursor(const ASTPtr & argument, const ContextPtr & context, const std::vector<DeltaInput> & inputs)
{
    auto [value, type] = evaluateConstantExpression(argument, context);
    auto converted = convertFieldToTypeOrThrow(value, *getCursorType(), type.get());

    DeltaCursor cursor;
    for (const auto & table : converted.safeGet<Map>())
    {
        const auto & table_tuple = table.safeGet<Tuple>();
        auto & partitions = cursor[table_tuple[0].safeGet<String>()];
        for (const auto & bound : table_tuple[1].safeGet<Map>())
        {
            const auto & bound_tuple = bound.safeGet<Tuple>();
            partitions[bound_tuple[0].safeGet<String>()] = bound_tuple[1].safeGet<UInt64>();
        }
    }

    for (const auto & [key, _] : cursor)
        if (!std::ranges::any_of(inputs, [&](const auto & input) { return input.key == key; }))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta query cursor has table {}, which the query does not read", key);
    for (const auto & input : inputs)
        if (!cursor.contains(input.key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta query cursor has no entry for table {}", input.key);
    return cursor;
}

/// Throws unless `lower` <= `upper` in every partition. A partition missing from a cursor has bound 0.
void checkCursorOrder(const DeltaCursor & lower, const DeltaCursor & upper, std::string_view lower_name, std::string_view upper_name)
{
    for (const auto & [key, partitions] : lower)
    {
        const auto upper_partitions = upper.find(key);
        for (const auto & [partition_id, bound] : partitions)
        {
            UInt64 upper_bound = 0;
            if (upper_partitions != upper.end())
                if (auto it = upper_partitions->second.find(partition_id); it != upper_partitions->second.end())
                    upper_bound = it->second;
            if (bound > upper_bound)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta query {} is ahead of {} in table {} partition {}: {} > {}",
                    lower_name, upper_name, key, partition_id, bound, upper_bound);
        }
    }
}

ASTPtr makeColumn(const std::vector<String> & qualifier, const String & column)
{
    auto parts = qualifier;
    parts.push_back(column);
    return make_intrusive<ASTIdentifier>(std::move(parts));
}

/// `_block_number <= bound(_partition_id)`, where partitions missing from `partitions` have bound 0 and so contribute no rows.
ASTPtr makeUpperBound(const std::vector<String> & qualifier, const std::map<String, UInt64> & partitions)
{
    Array ids;
    Array bounds;
    for (const auto & [partition_id, bound] : partitions)
    {
        ids.push_back(partition_id);
        bounds.push_back(bound);
    }
    auto bound = makeASTFunction("transform",
        makeColumn(qualifier, "_partition_id"),
        make_intrusive<ASTLiteral>(std::move(ids)),
        make_intrusive<ASTLiteral>(std::move(bounds)),
        make_intrusive<ASTLiteral>(UInt64(0)));
    return makeASTFunction("lessOrEquals", makeColumn(qualifier, "_block_number"), std::move(bound));
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
    const IAST & ast, const std::vector<DeltaInput> & inputs, const std::unordered_set<String> & expression_aliases)
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
bool isSemiJoinKeyPair(const InputColumn & a, const InputColumn & b, const std::vector<DeltaInput> & inputs)
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
std::vector<std::array<InputColumn, 2>> collectJoinEqualities(const ASTSelectQuery & select, const std::vector<DeltaInput> & inputs)
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
ASTPtr makeSemiJoinSubquery(const DeltaInput & input, const String & column, ASTPtr where)
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

/// The UNION ALL over i of Q, where input i reads rows in (old, new], inputs before it read rows <= old, and inputs after it
/// read rows <= new. That is the delta of a join of the inputs under the sequential update rule.
///
/// Term i also gets a semi-join reduction. The reduction makes the cost of term i follow the delta instead of the size of the
/// other inputs. A breadth-first search from input i follows the join equalities. Each input b reached from input a by `a.x = b.y` gets the conjunct
/// `b.y IN (SELECT x FROM a WHERE ...)`, where the subquery's WHERE holds the bounds and reduction of a in term i. Every
/// row of the term satisfies that conjunct, because its row of a satisfies the same bounds and reduction and has
/// `x = b.y`. So the conjunct removes only rows that the join removes. The conjunct only filters rows of b and never
/// changes how many times a row appears, so it stays correct for inputs whose rows carry negative weights.
ASTPtr buildDeltaQuery(
    const ASTSelectWithUnionQuery & query, const std::vector<DeltaInput> & inputs, const DeltaCursor & old_cursor, const DeltaCursor & new_cursor)
{
    const auto & select = getSingleSelect(query);
    const auto equalities = collectJoinEqualities(select, inputs);
    auto terms = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto add_bounds = [&](ASTs & conjuncts, size_t j, const std::vector<String> & qualifier)
        {
            const auto & key = inputs[j].key;
            if (j <= i)
                conjuncts.push_back(makeUpperBound(qualifier, j == i ? new_cursor.at(key) : old_cursor.at(key)));
            else
                conjuncts.push_back(makeUpperBound(qualifier, new_cursor.at(key)));
            if (j == i)
                conjuncts.push_back(makeASTFunction("not", makeUpperBound(qualifier, old_cursor.at(key))));
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

/// Shared by `deltaQuery` and `deltaQueryFrontier`, which both take Q as their first argument.
class TableFunctionDeltaQueryBase : public ITableFunction
{
protected:
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {0}; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr /*context*/) override
    {
        const auto & function = ast_function->as<ASTFunction &>();
        size_t num_arguments = function.arguments ? function.arguments->children.size() : 0;
        const IAST * query = num_arguments ? function.arguments->children[0].get() : nullptr;
        if (const auto * subquery = query ? query->as<ASTSubquery>() : nullptr)
            query = subquery->children.at(0).get();
        if (!query || !query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a query as its first argument", getName());
        if (num_arguments != 1 + numCursorArguments())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' takes a query and {} cursors",
                getName(), numCursorArguments());
        delta_query = query->clone();
        cursor_arguments.assign(function.arguments->children.begin() + 1, function.arguments->children.end());
    }

    virtual size_t numCursorArguments() const = 0;

    ASTPtr delta_query;
    ASTs cursor_arguments;
};

class TableFunctionDeltaQuery : public TableFunctionDeltaQueryBase
{
public:
    static constexpr auto name = "deltaQuery";
    std::string getName() const override { return name; }

private:
    size_t numCursorArguments() const override { return 2; }
    const char * getStorageEngineName() const override { return "View"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const override
    {
        return ColumnsDescription(InterpreterSelectQueryAnalyzer::getSampleBlock(delta_query, context)->getNamesAndTypesList());
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription, bool) const override
    {
        auto inputs = collectDeltaInputs(delta_query, context);
        auto old_cursor = parseCursor(cursor_arguments[0], context, inputs);
        auto new_cursor = parseCursor(cursor_arguments[1], context, inputs);
        checkCursorOrder(old_cursor, new_cursor, "old cursor", "new cursor");
        checkCursorOrder(new_cursor, computeFrontier(inputs), "new cursor", "the committed frontier");

        ASTCreateQuery create;
        create.set(create.select, buildDeltaQuery(delta_query->as<ASTSelectWithUnionQuery &>(), inputs, old_cursor, new_cursor));
        ColumnsDescription columns(InterpreterSelectQueryAnalyzer::getSampleBlock(create.select->clone(), context)->getNamesAndTypesList());
        auto storage = std::make_shared<StorageView>(StorageID(getDatabaseName(), table_name), create, columns, "");
        storage->startup();
        return storage;
    }
};

class TableFunctionDeltaQueryFrontier : public TableFunctionDeltaQueryBase
{
public:
    static constexpr auto name = "deltaQueryFrontier";
    std::string getName() const override { return name; }

private:
    size_t numCursorArguments() const override { return 0; }
    /// `StorageValues` is not a registered table engine, as in `TableFunctionValues`.
    const char * getStorageEngineName() const override { return ""; }

    ColumnsDescription getActualTableStructure(ContextPtr, bool) const override
    {
        return ColumnsDescription(NamesAndTypesList{{"cursor", getCursorType()}});
    }

    StoragePtr executeImpl(const ASTPtr &, ContextPtr context, const String & table_name, ColumnsDescription, bool) const override
    {
        auto type = getCursorType();
        auto column = type->createColumn();
        column->insert(cursorToField(computeFrontier(collectDeltaInputs(delta_query, context))));
        Block block{{std::move(column), type, "cursor"}};
        auto storage = std::make_shared<StorageValues>(
            StorageID(getDatabaseName(), table_name), getActualTableStructure(context, false), std::move(block));
        storage->startup();
        return storage;
    }
};

}

void registerTableFunctionDeltaQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDeltaQuery>({.description = R"DOCS_MD(
Returns the rows that a query gains from the blocks inserted between two cursors.

`deltaQuery(query, old_cursor, new_cursor)` evaluates `query` over the rows of its tables with `old_cursor < _block_number <= new_cursor`
joined with the other tables' rows at the matching cursor. Merging the results over a chain of cursors gives `query` evaluated at the last
cursor. Aggregates in the result are partial and merge like `SummingMergeTree` or `AggregatingMergeTree`.

A cursor has type `Map(String, Map(String, UInt64))`. It maps each table of the query, as `'database.table'` with names backquoted where
needed, to the highest included block number of each partition. Get the current cursor with `deltaQueryFrontier`.

The tables must be plain `MergeTree` tables with `enable_block_number_column = 1`, without mutations or lightweight updates.
The query may use filters, `INNER` and `CROSS` joins, and `GROUP BY`. Each result column is either free of aggregates or one call of `sum`,
`count`, their `-If` forms, or any `-State` aggregate.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionDeltaQueryFrontier>({.description = R"DOCS_MD(
Returns one row with column `cursor`, the highest committed block number of every partition of every table of a query.
Pass it to `deltaQuery` as a cursor.
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, SecretArgumentsSpec{}, {.allow_readonly = true});
}

}
