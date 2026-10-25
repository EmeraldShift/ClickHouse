#include <Storages/MergeTree/Streaming/DeltaQuery.h>
#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Core/Joins.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageProxy.h>

#include <base/find_symbols.h>
#include <boost/algorithm/string/trim.hpp>

#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsFloat delta_query_asof_lookback;
    extern const SettingsString delta_query_lookup_tables;
    extern const SettingsString delta_query_sign_column;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_block_number_column;
}

using namespace DeltaQueryInternal;

namespace
{

/// The inputs by their nodes in the query tree.
using InputIndexes = std::unordered_map<const IQueryTreeNode *, size_t>;

/// `value * weight`, where `argument` is the node of `value` in the query tree.
ASTPtr makeWeightedValue(const ASTPtr & value, const QueryTreeNodePtr & argument, const ASTPtr & weight)
{
    /// The product of an unsigned integer and a signed weight has the signed type of the same width. That type can't hold
    /// every value of the unsigned type. So the value is first converted to the signed type of twice the width.
    auto weighted = value;
    WhichDataType type(removeLowCardinalityAndNullable(argument->getResultType()));
    if (type.isUInt64())
        weighted = makeASTFunction("toInt128", weighted);
    else if (type.isUInt128())
        weighted = makeASTFunction("toInt256", weighted);
    else if (type.isUInt256())
        throwUnsupported("aggregates of UInt256 over signed inputs");
    return makeASTFunction("multiply", weighted, weight->clone());
}

/// The aggregate `function` with each row weighted by `weight`. `isInvertibleAggregate` must accept `function`, and `node`
/// is its node in the query tree. The result over rows of weight 1 and -1 is the result of `function` over the rows of
/// weight 1 that no row of weight -1 cancels.
ASTPtr makeWeightedAggregate(const ASTFunction & function, const FunctionNode & node, const ASTPtr & weight)
{
    const auto [base, suffixes] = splitCombinators(function.name);
    ASTs arguments = function.arguments ? function.arguments->children : ASTs{};
    const auto & argument_nodes = node.getArguments().getNodes();
    if (base == "exponentialMovingAverage")
    {
        /// The state is a sum of the values decayed to the latest time of its rows. A row of weight -1 cancels the term of
        /// its row of weight 1. The latest time stays the same, so the average is as of the latest time of any row of the
        /// group, including canceled rows.
        auto result = function.clone();
        auto & result_arguments = result->as<ASTFunction &>().arguments->children;
        result_arguments.at(0) = makeWeightedValue(result_arguments.at(0), argument_nodes.at(0), weight);
        return result;
    }

    ASTs conditions;
    if (!suffixes.empty() && !arguments.empty())
    {
        conditions.push_back(arguments.back());
        arguments.pop_back();
    }
    ASTPtr value;
    if (base == "sum" && arguments.size() == 1)
        value = makeWeightedValue(arguments[0], argument_nodes.at(0), weight);
    else if (base == "count" && arguments.size() <= 1)
    {
        value = weight->clone();
        if (!arguments.empty())
            conditions.insert(conditions.begin(), makeASTFunction("isNotNull", arguments[0]));
    }
    else
        throwUnsupported(fmt::format("aggregate function {} with {} arguments over signed inputs", function.name, arguments.size()));

    if (conditions.empty())
        return makeASTFunction("sum", std::move(value));
    auto condition = conditions.size() == 1 ? conditions[0] : makeASTFunction("and", std::move(conditions));
    return makeASTFunction("sumIf", std::move(value), std::move(condition));
}

/// Appends the tables of the join tree in order, the ON expressions of its joins, and its joins that are linear only in
/// their left side. Throws unless each join is an inner join, which is linear in both sides, or a LEFT, SEMI, ANTI or ASOF
/// join of a table by ON.
void collectJoinTree(const QueryTreeNodePtr & node, QueryTreeNodes & tables, QueryTreeNodes & join_conditions,
    QueryTreeNodes & keyed_joins)
{
    switch (node->getNodeType())
    {
        case QueryTreeNodeType::TABLE:
            tables.push_back(node);
            return;
        case QueryTreeNodeType::ARRAY_JOIN:
            collectJoinTree(node->as<ArrayJoinNode &>().getTableExpressionNode(), tables, join_conditions, keyed_joins);
            return;
        case QueryTreeNodeType::CROSS_JOIN:
        {
            auto & join = node->as<CrossJoinNode &>();
            if (std::ranges::any_of(join.getJoinTypes(), [](const auto & type) { return type.locality == JoinLocality::Global; }))
                throwUnsupported("GLOBAL joins");
            for (const auto & table : join.getTableExpressions())
                collectJoinTree(table, tables, join_conditions, keyed_joins);
            return;
        }
        case QueryTreeNodeType::JOIN:
        {
            /// Resolution has replaced an unspecified strictness by `join_default_strictness`.
            const auto & join = node->as<JoinNode &>();
            const auto kind = join.getKind();
            const auto strictness = join.getStrictness();
            const bool is_keyed = (kind == JoinKind::Left
                    && (strictness == JoinStrictness::All || strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti
                        || strictness == JoinStrictness::Asof))
                || (kind == JoinKind::Inner && strictness == JoinStrictness::Asof);
            if (!is_keyed && (kind != JoinKind::Inner || strictness != JoinStrictness::All))
                throwUnsupported("joins other than INNER ALL, LEFT ALL, LEFT SEMI, LEFT ANTI, ASOF, CROSS and comma joins");
            if (join.getLocality() == JoinLocality::Global)
                throwUnsupported("GLOBAL joins");
            const auto right_type = join.getRightTableExpressionNode()->getNodeType();
            if (is_keyed && (right_type != QueryTreeNodeType::TABLE || !join.isOnJoinExpression()))
                throwUnsupported("LEFT, SEMI, ANTI or ASOF joins whose right side isn't a table joined by ON");
            collectJoinTree(join.getLeftTableExpressionNode(), tables, join_conditions, keyed_joins);
            collectJoinTree(join.getRightTableExpressionNode(), tables, join_conditions, keyed_joins);
            if (join.isOnJoinExpression())
                join_conditions.push_back(join.getJoinExpression());
            if (is_keyed)
                keyed_joins.push_back(node);
            return;
        }
        default:
            throwUnsupported("table functions or subqueries in FROM");
    }
}

/// Throws unless the table has block numbers that order its rows by insert, and its merges keep its rows.
Input makeInput(const TableNode & table)
{
    if (const auto & modifiers = table.getTableExpressionModifiers();
        modifiers && (modifiers->hasSampleSizeRatio() || modifiers->hasStream()))
        throwUnsupported("SAMPLE or STREAM");

    auto storage = castStorage<MergeTreeData>(table.getStorage(), DeferredTable::Load);
    if (!storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries read only MergeTree tables, but {} has engine {}",
            table.getStorageID().getNameForLogs(), table.getStorage()->getName());
    const auto table_id = storage->getStorageID();
    if (storage->merging_params.mode != MergeTreeData::MergingParams::Ordinary)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, whose merges rewrite rows",
            table_id.getNameForLogs());
    if (!(*storage->getSettings())[MergeTreeSetting::enable_block_number_column])
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Delta queries require table {} to set enable_block_number_column = 1",
            table_id.getNameForLogs());
    if (!table_id.hasUUID())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries require table {} to have a UUID, as tables of Atomic databases do",
            table_id.getNameForLogs());
    /// The bounds read the virtual columns `_part` and `_partition_id`, which columns of the same names would shadow.
    for (const auto * virtual_column : {"_part", "_partition_id"})
        if (table.getStorageSnapshot()->metadata->getColumns().has(virtual_column))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support table {}, whose column {} shadows a virtual column",
                table_id.getNameForLogs(), virtual_column);

    const bool updatable = !storage->supportsReplication() && storage->supportsLightweightUpdate().has_value();
    return {.alias = table.getAlias(), .key = toString(table_id.uuid), .storage = std::move(storage), .updatable = updatable};
}

/// Rejects subqueries, which could read tables that the cursors do not cover. Resolution replaces scalar subqueries by
/// their values, so this check runs on the AST.
void checkNoSubqueries(const IAST & ast)
{
    for (const auto & child : ast.children)
    {
        if (child->as<ASTSubquery>())
            throwUnsupported("subqueries");
        checkNoSubqueries(*child);
    }
}

/// Rejects tables outside FROM, like in `k IN t`, which the cursors do not cover, and window functions, whose result over
/// the union of the terms differs from the union of their results.
void checkExpressions(const IQueryTreeNode & node, const InputIndexes & input_indexes)
{
    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;
        auto type = child->getNodeType();
        if (type == QueryTreeNodeType::TABLE && !input_indexes.contains(child.get()))
            throwUnsupported("IN over tables");
        if (const auto * function = child->as<FunctionNode>(); function && function->isWindowFunction())
            throwUnsupported(fmt::format("window functions in {}", linear_queries));
        checkExpressions(*child, input_indexes);
    }
}

/// Checks the clauses whose result over the union of the terms of a linear query differs from the union of their results.
void checkLinearClauses(const QueryNode & query)
{
    if (query.isDistinct())
        throwUnsupported(fmt::format("DISTINCT in {}", linear_queries));
    if (query.isGroupByWithTotals() || query.isGroupByWithRollup() || query.isGroupByWithCube() || query.isGroupByWithGroupingSets())
        throwUnsupported(fmt::format("WITH TOTALS, ROLLUP, CUBE or GROUPING SETS in {}", linear_queries));
    if (query.hasHaving())
        throwUnsupported(fmt::format("HAVING in {}", linear_queries));
    if (query.hasOrderBy() || query.hasLimit() || query.hasOffset() || query.hasLimitByLimit() || query.hasLimitAfter()
        || query.hasLimitUntil())
        throwUnsupported(fmt::format("ORDER BY, LIMIT or LIMIT BY in {}", linear_queries));
    if (query.hasWindow() || query.hasQualify())
        throwUnsupported(fmt::format("WINDOW or QUALIFY in {}", linear_queries));
}

/// Checks that each result column of a linear query is either an aggregate whose deltas merge or free of aggregates. Over
/// signed inputs the aggregate must also be invertible. Resolution inlines aliases, so `sum(v) AS s, s + 1` fails too. The
/// query tree skips optimizations, which move arithmetic out of aggregates and would make `sum(v * 2)` fail.
void checkResultColumns(const QueryNode & query, bool is_signed)
{
    for (const auto & column : query.getProjection().getNodes())
    {
        if (const auto * function = column->as<FunctionNode>(); function && function->isAggregateFunction())
        {
            const auto & name = function->getFunctionName();
            if (is_signed && !isInvertibleAggregate(name))
                throwUnsupported(fmt::format("aggregate function {} over signed inputs, use count, sum, their -If forms or exponentialMovingAverageState instead", name));
            if (!is_signed && !isMergeableAggregate(name))
                throwUnsupported(fmt::format("aggregate function {} in {}, use its -State form instead", name, linear_queries));
        }
        else if (hasAggregateFunctionNodes(column))
            throwUnsupported(fmt::format("expressions over aggregates in {}, select each aggregate as its own column", linear_queries));
    }
}

/// Whether the table has the sign column. Throws if that column isn't an ordinary column of a signed integer type.
bool hasSignColumn(const TableNode & table, const String & sign_column)
{
    const auto & columns = table.getStorageSnapshot()->metadata->getColumns();
    if (!columns.has(sign_column))
        return false;
    auto column = columns.tryGetPhysical(sign_column);
    if (!column || !WhichDataType(column->type).isNativeInt())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Column {} of table {} must be an ordinary column of type Int8, Int16, Int32 or Int64 to be the sign column of delta queries",
            sign_column, table.getStorageID().getNameForLogs());
    return true;
}

/// Rejects reading the sign column of a signed table. The deltas turn the signs into weights, and the query can't see them.
void checkSignColumnNotRead(const IQueryTreeNode & node, const std::unordered_set<const IQueryTreeNode *> & signed_tables,
    const String & sign_column)
{
    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;
        if (const auto * column = child->as<ColumnNode>(); column && column->getColumnName() == sign_column)
            if (auto source = column->getColumnSourceOrNull(); source && signed_tables.contains(source.get()))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Delta queries do not support reading the sign column {}, which their deltas compute. List the other columns instead of *",
                    sign_column);
        checkSignColumnNotRead(*child, signed_tables, sign_column);
    }
}

/// The edges of the top-level conjuncts `a.x = b.y` of `conditions` that equate columns of two different inputs.
///
/// The semi-join reduction rewrites `a.x = b.y` into `b.y IN (SELECT x FROM a ...)`. `IN` casts `b.y` to the type of `x`
/// and looks it up by its binary representation. That lookup can disagree with `=`. For example, `-0.0 = 0.0` is true and
/// `-0.0 IN (SELECT 0.0)` is false. So only columns of one type whose equal values have one representation count as join
/// keys, like integers, strings, UUIDs and decimals. Floats don't.
std::vector<SemiJoinEdge> collectSemiJoinEdges(const QueryTreeNodes & conditions, const InputIndexes & input_indexes)
{
    QueryTreeNodes conjuncts;
    for (const auto & condition : conditions)
        splitConjuncts(condition, conjuncts);

    std::vector<SemiJoinEdge> edges;
    for (const auto & conjunct : conjuncts)
    {
        const auto * function = conjunct->as<FunctionNode>();
        if (!function || function->getFunctionName() != "equals" || function->getArguments().getNodes().size() != 2)
            continue;
        const auto * a = function->getArguments().getNodes()[0]->as<ColumnNode>();
        const auto * b = function->getArguments().getNodes()[1]->as<ColumnNode>();
        if (!a || !b)
            continue;
        auto a_input = input_indexes.find(a->getColumnSourceOrNull().get());
        auto b_input = input_indexes.find(b->getColumnSourceOrNull().get());
        if (a_input == input_indexes.end() || b_input == input_indexes.end() || a_input->second == b_input->second)
            continue;
        auto type = removeLowCardinalityAndNullable(a->getColumnType());
        if (!type->equals(*removeLowCardinalityAndNullable(b->getColumnType()))
            || !(type->isValueRepresentedByInteger() || isStringOrFixedString(type) || isUUID(type) || isDecimal(type)
                || isDateTime64(type) || isIPv4(type) || isIPv6(type)))
            continue;
        edges.push_back({a_input->second, a->getColumnName(), b_input->second, b->getColumnName(), a->getColumnType()->getName()});
        edges.push_back({b_input->second, b->getColumnName(), a_input->second, a->getColumnName(), b->getColumnType()->getName()});
    }
    return edges;
}

/// Returns `lookback`, see `delta_query_asof_lookback`, as an amount to add to or subtract from a value of `type`. Returns
/// null for other types and for a lookback too large for an Int64.
ASTPtr makeAsofLookback(const DataTypePtr & type, Float64 lookback)
{
    WhichDataType which(type);
    const Float64 amount = std::ceil(which.isDateTime64() ? lookback * 1e6 : lookback);
    if (!(amount < 1e18))
        return nullptr;
    auto literal = make_intrusive<ASTLiteral>(Field(static_cast<Int64>(amount)));
    if (which.isDateTime64())
        return makeASTFunction("toIntervalMicrosecond", std::move(literal));
    if (which.isDateTime())
        return makeASTFunction("toIntervalSecond", std::move(literal));
    /// An Int64 keeps `t - lookback` from wrapping around for unsigned columns.
    if (which.isNativeInt() || which.isNativeUInt())
        return makeASTFunction("toInt64", std::move(literal));
    return nullptr;
}

/// The ASOF join of the right input `right` by `join`, unless its ON has other conjuncts or its time columns have a type
/// without a lookback, see `makeAsofLookback`. The equalities must be between join keys of `right`.
std::optional<DeltaQuery::AsofJoin> makeAsofJoin(const JoinNode & join, size_t right, const DeltaQuery::Input & right_input,
    const InputIndexes & input_indexes, Float64 lookback)
{
    QueryTreeNodes conjuncts;
    splitConjuncts(join.getJoinExpression(), conjuncts);
    DeltaQuery::AsofJoin asof;
    asof.right = right;
    std::optional<size_t> left;
    bool has_time = false;
    for (const auto & conjunct : conjuncts)
    {
        const auto * function = conjunct->as<FunctionNode>();
        if (!function || function->getArguments().getNodes().size() != 2)
            return {};
        const auto * a = function->getArguments().getNodes()[0]->as<ColumnNode>();
        const auto * b = function->getArguments().getNodes()[1]->as<ColumnNode>();
        if (!a || !b)
            return {};
        auto a_input = input_indexes.find(a->getColumnSourceOrNull().get());
        auto b_input = input_indexes.find(b->getColumnSourceOrNull().get());
        if (a_input == input_indexes.end() || b_input == input_indexes.end())
            return {};
        String name = function->getFunctionName();
        if (a_input->second == right)
        {
            std::swap(a, b);
            std::swap(a_input, b_input);
            static const std::map<String, String> flipped = {{"equals", "equals"}, {"greaterOrEquals", "lessOrEquals"},
                {"greater", "less"}, {"lessOrEquals", "greaterOrEquals"}, {"less", "greater"}};
            auto it = flipped.find(name);
            if (it == flipped.end())
                return {};
            name = it->second;
        }
        if (b_input->second != right || a_input->second == right || (left && *left != a_input->second))
            return {};
        left = a_input->second;
        if (name == "equals")
        {
            if (!std::ranges::contains(right_input.join_keys, b->getColumnName()))
                return {};
            asof.left_keys.push_back(a->getColumnName());
            asof.right_keys.push_back(b->getColumnName());
            continue;
        }
        if (has_time || (name != "greaterOrEquals" && name != "greater" && name != "lessOrEquals" && name != "less"))
            return {};
        has_time = true;
        asof.left_time = a->getColumnName();
        asof.right_time = b->getColumnName();
        asof.backward = name.starts_with("greater");
        asof.strict = !name.ends_with("OrEquals");
        const auto type = removeLowCardinality(a->getColumnType());
        auto amount = makeAsofLookback(type, lookback);
        if (!type->equals(*removeLowCardinality(b->getColumnType())) || !amount)
            return {};
        asof.lookback = std::move(amount);
    }
    if (!left || !has_time || asof.left_keys.empty())
        return {};
    asof.left = *left;
    return asof;
}

/// Rewrites the resolved `select` of `query` to carry the weights of its rows, the product of the signs of the signed
/// inputs. A query without aggregates or GROUP BY returns the weight of each row as `sign_column`, unless it has no signs and
/// not `unit_sign`. A query with aggregates weighs the rows in its aggregates, and is unchanged without signed inputs.
void addWeights(ASTSelectQuery & select, const QueryNode & query, const ASTs & signs, const String & sign_column, bool unit_sign)
{
    const auto & nodes = query.getProjection().getNodes();
    const auto & columns = query.getProjectionColumns();
    auto & projection = select.select()->children;
    chassert(projection.size() == nodes.size() && columns.size() == nodes.size());

    ASTPtr weight = signs.empty() ? make_intrusive<ASTLiteral>(Field(Int64{1})) : signs[0];
    for (size_t i = 1; i < signs.size(); ++i)
        weight = makeASTFunction("multiply", weight, signs[i]);

    if (!query.hasGroupBy() && !std::ranges::any_of(nodes, hasAggregateFunctionNodes))
    {
        if (std::ranges::any_of(columns, [&](const auto & column) { return column.name == sign_column; }))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The delta query returns column {}, which delta_query_sign_column names", sign_column);
        if (signs.empty() && !unit_sign)
            return;
        auto sign = makeASTFunction("toInt8", weight);
        sign->setAlias(sign_column);
        projection.push_back(std::move(sign));
        return;
    }
    if (signs.empty())
        return;

    bool has_aggregates = false;
    for (size_t i = 0; i < nodes.size(); ++i)
    {
        const auto * function = nodes[i]->as<FunctionNode>();
        if (!function || !function->isAggregateFunction())
            continue;
        /// The alias keeps the name of the result column.
        projection[i] = makeWeightedAggregate(projection[i]->as<ASTFunction &>(), *function, weight);
        projection[i]->setAlias(columns[i].name);
        has_aggregates = true;
    }
    /// A group of the target table would stay after its last row is retracted.
    if (!has_aggregates)
        throwUnsupported("GROUP BY without aggregates over signed inputs, add count() to see which groups have rows");
}

}

DeltaQuery::DeltaQuery(const ASTPtr & query_, const ContextPtr & context_)
{
    /// The query can set `delta_query_sign_column` in its SETTINGS, as the query of a view does.
    auto context = Context::createCopy(context_);
    InterpreterSetQuery::applySettingsFromQuery(query_, context);
    /// The deltas weigh their rows by a sign column. Without `delta_query_sign_column`, no input has a sign column, and the
    /// sign column of the deltas has an internal name.
    String sign_column_setting = context->getSettingsRef()[Setting::delta_query_sign_column];
    if (sign_column_setting.empty())
    {
        sign_column_setting = internal_sign_column;
        context->setSetting("delta_query_sign_column", sign_column_setting);
    }
    checkNoSubqueries(*query_);
    analysis_context = context;

    analyze(query_, context, sign_column_setting);
}

DeltaQuery::~DeltaQuery() = default;

void DeltaQuery::checkResultSign() const
{
    if (sign_column == internal_sign_column && getResultColumns().contains(sign_column))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The delta query retracts rows, like the rows of LEFT, SEMI and ANTI joins, of lightweight updates or of filters by "
            "now(). The setting delta_query_sign_column must name the sign column of its result");
}

void DeltaQuery::analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column_)
{
    inputs.clear();
    asof_joins.clear();
    const auto asof_lookback = static_cast<Float64>(context->getSettingsRef()[Setting::delta_query_asof_lookback]);
    clock_input.reset();
    clock_conjuncts.clear();
    consolidated_columns.clear();
    summed_columns.clear();
    sign_column = sign_column_;

    /// A filter by now() changes its result as time passes, see `buildDelta`. Elsewhere now() would change rows that no
    /// delta reads again.
    const bool has_clock = callsNow(*query_);
    if (const auto & selects = query_->as<ASTSelectWithUnionQuery &>().list_of_selects->children; has_clock && selects.size() == 1)
    {
        const auto & original = selects[0]->as<ASTSelectQuery &>();
        if (std::ranges::any_of(original.children, [&](const auto & child) { return child != original.where() && callsNow(*child); }))
            throwUnsupported("now() outside WHERE");
    }
    /// Resolution folds now() into a constant, and its source expression keeps the call.
    ConvertToASTOptions ast_options;
    ast_options.use_source_expression_for_constants = has_clock;
    InterpreterSelectQueryAnalyzer interpreter(query_, context, SelectQueryOptions().analyze().ignoreASTOptimizations());
    auto query_tree = interpreter.getQueryTree()->clone();

    /// Aliases and columns of the query can shadow the names of its tables. So the bounds qualify the columns of each table
    /// by its unique alias `__tableN`.
    createUniqueAliasesIfNecessary(query_tree, context);
    const auto * query_node = query_tree->as<QueryNode>();
    if (!query_node)
        throwUnsupported("UNION");

    QueryTreeNodes tables;
    QueryTreeNodes conditions;
    QueryTreeNodes keyed_joins;
    collectJoinTree(query_node->getJoinTreeNode(), tables, conditions, keyed_joins);
    InputIndexes input_indexes;
    std::set<String> lookup_tables;
    std::vector<std::string> names;
    splitInto<','>(names, context->getSettingsRef()[Setting::delta_query_lookup_tables].value);
    for (auto & name : names)
    {
        boost::trim(name);
        if (!name.empty())
            lookup_tables.insert(name);
    }
    std::set<String> found_lookup_tables;
    for (const auto & table : tables)
    {
        input_indexes.emplace(table.get(), inputs.size());
        inputs.push_back(makeInput(table->as<TableNode &>()));
        const auto table_id = inputs.back().storage->getStorageID();
        for (const auto & name : {table_id.table_name, table_id.getFullNameNotQuoted()})
            if (lookup_tables.contains(name))
            {
                inputs.back().lookup = true;
                found_lookup_tables.insert(name);
            }
    }
    for (const auto & name : lookup_tables)
        if (!found_lookup_tables.contains(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "delta_query_lookup_tables names {}, which the delta query doesn't read", name);
    if (std::ranges::all_of(inputs, [](const auto & input) { return input.lookup; }))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "delta_query_lookup_tables names every input of the delta query");

    /// A SEMI or ANTI join returns each left row at most once, whatever the signs of its matches.
    std::unordered_set<size_t> existence_joins;
    /// The equalities of the ON of each such join, toward its right table.
    std::vector<SemiJoinEdge> keyed_edges;
    for (const auto & node : keyed_joins)
    {
        const auto & join = node->as<JoinNode &>();
        const size_t right = input_indexes.at(join.getRightTableExpressionNode().get());
        for (const auto & edge : collectSemiJoinEdges({join.getJoinExpression()}, input_indexes))
        {
            if (edge.to == right)
                keyed_edges.push_back(edge);
            if (edge.from == right && std::ranges::find(inputs[right].join_keys, edge.from_column) == inputs[right].join_keys.end())
                inputs[right].join_keys.push_back(edge.from_column);
        }
        if (inputs[right].join_keys.empty())
            throwUnsupported("LEFT, SEMI, ANTI or ASOF joins without an equality of join keys of both sides in ON");
        if (join.getStrictness() == JoinStrictness::Semi || join.getStrictness() == JoinStrictness::Anti)
            existence_joins.insert(right);
        if (join.getStrictness() == JoinStrictness::Asof && asof_lookback > 0)
            if (auto asof = makeAsofJoin(join, right, inputs[right], input_indexes, asof_lookback))
                asof_joins.push_back(std::move(*asof));
    }
    /// The delta of a join that isn't linear in a table, of a filter by now(), or of lightweight updates retracts rows of the
    /// result.
    const bool retracts = has_clock || std::ranges::any_of(inputs, [&](const auto & input)
    {
        return !input.lookup && (!input.join_keys.empty() || input.updatable);
    });

    /// The deltas must merge into the query over the union of the inputs.
    ASTs signs;
    std::unordered_set<const IQueryTreeNode *> signed_tables;
    for (size_t i = 0; i < tables.size(); ++i)
    {
        if (!hasSignColumn(tables[i]->as<TableNode &>(), sign_column))
            continue;
        inputs[i].is_signed = true;
        signed_tables.insert(tables[i].get());
        auto sign = makeColumn(inputs[i].alias, sign_column);
        if (existence_joins.contains(i))
            continue;
        /// A left row without a match gets sign 0 from the right side.
        if (!inputs[i].join_keys.empty())
            sign = makeASTFunction("if", makeASTFunction("equals", sign->clone(), make_intrusive<ASTLiteral>(Field(Int64{0}))),
                make_intrusive<ASTLiteral>(Field(Int64{1})), sign);
        signs.push_back(std::move(sign));
    }
    if (retracts)
        signs.push_back(make_intrusive<ASTIdentifier>(term_sign_name));
    checkSignColumnNotRead(*query_tree, signed_tables, sign_column);
    /// The rows of a signed or updated right input near a time can cancel or change, so their existence there says
    /// nothing about the match. A left input with join keys is itself the right side of a join. Its terms read all the
    /// rows of the touched keys and not just the changes, and the defaults that pad it don't pass its conjuncts.
    std::erase_if(asof_joins, [&](const auto & asof)
    {
        return inputs[asof.right].is_signed || inputs[asof.right].updatable || !inputs[asof.left].join_keys.empty();
    });
    checkExpressions(*query_tree, input_indexes);
    checkLinearClauses(*query_node);
    checkResultColumns(*query_node, !signs.empty());

    if (query_node->hasWhere())
        conditions.push_back(query_node->getWhere());
    semi_join_edges = collectSemiJoinEdges(conditions, input_indexes);
    /// A reduction of the right table of a LEFT, SEMI, ANTI or ASOF join by an equality outside its ON could remove the match
    /// of a left row, which would then join with default values instead.
    std::erase_if(semi_join_edges, [&](const auto & edge)
    {
        return !inputs[edge.to].join_keys.empty() && !std::ranges::contains(keyed_edges, edge);
    });
    for (auto & edge : semi_join_edges)
        edge.to_left_side = std::ranges::any_of(keyed_edges, [&](const auto & keyed)
        {
            return keyed.from == edge.to && keyed.from_column == edge.to_column && keyed.to == edge.from && keyed.to_column == edge.from_column;
        });
    query = query_tree->toAST(ast_options);

    auto & select = query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
    /// Without GROUP BY, a delta that reads no rows still returns one row by default, with 0 for `min` and `max`.
    if (!select.groupBy())
        setSetting(select, "empty_result_for_aggregation_by_empty_set", true);
    /// The rows for a target with a sign column have a sign even if no input has one.
    addWeights(select, *query_node, signs, sign_column, sign_column != internal_sign_column);

    std::map<String, std::set<String>> columns;
    collectColumns(select, columns);
    for (auto & input : inputs)
        if (!input.join_keys.empty())
            for (const auto & column : columns[input.alias])
                if (column != sign_column)
                    input.columns.push_back(column);

    /// A signed query without aggregates returns the sign column. The pairs of terms of its delta return most rows twice with
    /// opposite signs.
    if (!signs.empty() && select.select()->children.back()->tryGetAlias() == sign_column)
    {
        for (const auto & column : query_node->getProjectionColumns())
            consolidated_columns.push_back(column.name);
    }
    else if (!signs.empty())
    {
        const auto & nodes = query_node->getProjection().getNodes();
        const auto projection_columns = query_node->getProjectionColumns();
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            const auto * function = nodes[i]->as<FunctionNode>();
            const bool is_aggregate = function && function->isAggregateFunction();
            if (is_aggregate && !std::ranges::contains(std::array{"sum", "count"}, splitCombinators(function->getFunctionName()).first))
            {
                summed_columns.clear();
                break;
            }
            summed_columns.emplace_back(projection_columns[i].name, is_aggregate);
        }
    }

    if (has_clock)
    {
        ASTs conjuncts{select.where()};
        if (const auto * function = select.where()->as<ASTFunction>(); function && function->name == "and")
            conjuncts = function->arguments->children;
        std::map<String, std::set<String>> clocked;
        for (const auto & conjunct : conjuncts)
            if (callsNow(*conjunct))
            {
                clock_conjuncts.push_back(conjunct->clone());
                collectColumns(*conjunct, clocked);
            }
        auto it = clocked.size() == 1
            ? std::ranges::find_if(inputs, [&](const auto & input) { return input.alias == clocked.begin()->first; })
            : inputs.end();
        if (it == inputs.end() || !it->join_keys.empty())
            throwUnsupported("filters by now() unless they read one table, which isn't the right side of a LEFT, SEMI, ANTI or ASOF join");
        clock_input = it - inputs.begin();
    }

    /// Resolution checks only that the user can see the tables. Building the plan checks the grants on the columns that
    /// the query reads.
    interpreter.getSampleBlock();
}

ASTPtr DeltaQuery::getQuery() const
{
    /// The weights of the query read `__term_sign`, which each term of a delta binds. The query itself is one term of sign 1.
    auto result = query->clone();
    bindTerm(*result, 1, {});
    return result;
}

NamesAndTypesList DeltaQuery::getResultColumns() const
{
    /// The passes after resolution keep the types of the result columns. Resolution alone finds them.
    InterpreterSelectQueryAnalyzer interpreter(getQuery(), analysis_context, SelectQueryOptions().analyze().ignoreASTOptimizations());
    const auto & columns = interpreter.getQueryTree()->as<const QueryNode &>().getProjectionColumns();
    return NamesAndTypesList(columns.begin(), columns.end());
}

ASTs DeltaQuery::castToResultTypes(const Names & columns) const
{
    const auto types = getResultColumns();
    ASTs casts;
    for (const auto & column : columns)
        casts.push_back(castTo(column, types));
    return casts;
}

}
