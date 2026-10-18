#include <Storages/MergeTree/Streaming/DeltaQuery.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Core/Joins.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>
#include <Storages/StorageProxy.h>

#include <boost/algorithm/string/replace.hpp>
#include <fmt/ranges.h>

#include <deque>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsString delta_query_sign_column;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_block_number_column;
}

/// One table of the FROM clause of the query.
struct DeltaQuery::Input
{
    /// The unique alias of the table in the query, which qualifies its columns.
    String alias;
    /// The key of the table in cursors: its UUID.
    String key;
    std::shared_ptr<MergeTreeData> storage;
};

/// An equality `from.from_column = to.to_column` between join keys of two inputs. The semi-join reduction follows it from
/// input `from` to input `to`.
struct DeltaQuery::SemiJoinEdge
{
    size_t from;
    String from_column;
    size_t to;
    String to_column;
};

/// The residual of a query and how to evaluate it over the integral of the core. The integral has a column `__key_i` per
/// key and a column `__agg_m` per aggregate of the core, where `__agg_0` is `count()`.
struct DeltaQuery::Residual
{
    String sign_column;
    /// Whether the keys are GROUP BY keys. Otherwise they are the columns that the residual reads, and each row of the
    /// integral stands for `__agg_0` rows.
    bool has_aggregates = false;
    size_t num_keys = 0;
    /// The function of the `SimpleAggregateFunction` of each aggregate column of the integral, or empty for a state.
    Strings simple_functions;
    /// The merge of each aggregate `__val_j` of the residual, which reads the aggregate columns of the integral.
    ASTs merges;
    /// The residual as a select over the placeholder table `residual_input`.
    String outer;
    Names outputs;
    /// The keys whose values bound the rows that a change of the integral can change, and the output columns that equal
    /// them. A refresh recomputes the rows of the touched values of these keys. Empty if it recomputes all rows.
    std::vector<size_t> partition_keys;
    Names partition_outputs;

    /// Filled from the analysis of the core.
    DataTypes key_types;
    NamesAndTypesList integral_columns;
    /// `merge AS __val_j` for each merge.
    Strings values;
    ASTPtr output_query;

    /// The rows of `integral` that pass `where` merged per key.
    String buildInput(const String & integral, const String & where) const;
    /// The residual over the rows of `integral` that pass `where`.
    String buildResidual(const String & integral, const String & where) const;
    /// `(c_1, ...) IN (SELECT __key_p1, ... FROM delta)`, where column c_i equals partition key i.
    String buildPartitionFilter(const Names & columns, const String & delta) const;
};

namespace
{

using Input = DeltaQuery::Input;
using SemiJoinEdge = DeltaQuery::SemiJoinEdge;

/// Table UUID -> partition ID -> first excluded block number.
using Cursor = std::map<String, std::map<String, Int64>>;

/// The queries whose deltas must merge into the query over the union of their inputs. Error messages name them.
constexpr std::string_view linear_queries = "joins or queries with delta_query_sign_column";

/// The inputs by their nodes in the query tree.
using InputIndexes = std::unordered_map<const IQueryTreeNode *, size_t>;

[[noreturn]] void throwUnsupported(std::string_view construct)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support {}", construct);
}

/// The name of the aggregate without its combinators, and the names of its combinators from the outermost.
std::pair<String, std::vector<String>> splitCombinators(const String & name)
{
    const auto & combinators = AggregateFunctionCombinatorFactory::instance();
    String base = name;
    std::vector<String> suffixes;
    while (auto combinator = combinators.tryFindSuffix(base))
    {
        suffixes.push_back(combinator->getName());
        base.resize(base.size() - combinator->getName().size());
    }
    return {AggregateFunctionFactory::instance().getCanonicalNameIfAny(base), std::move(suffixes)};
}

/// Aggregates that `makeWeightedAggregate` rewrites. Their results are sums over rows. A row of weight -1 cancels its term.
bool isInvertibleAggregate(const String & name)
{
    const auto [base, suffixes] = splitCombinators(name);
    if (base == "sum" || base == "count")
        return suffixes.empty() || suffixes == std::vector<String>{"If"};
    return base == "exponentialMovingAverage" && suffixes == std::vector<String>{"State"};
}

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

/// Aggregates whose results over disjoint inputs merge into the result over their union. A join term without GROUP BY
/// returns no row when it reads no rows. Only the groups of the terms then need to merge. A group can have no rows that
/// pass the condition of an `-If` form, and `minIf` or `maxIf` then returns 0. So only `sum` and `count` take `-If`.
bool isMergeableAggregate(const String & name)
{
    const auto [base, suffixes] = splitCombinators(name);
    if (!suffixes.empty() && suffixes.front() == "State")
        return true;
    if (!std::ranges::all_of(suffixes, [](const auto & suffix) { return suffix == "If"; }))
        return false;
    if (base == "sum" || base == "count")
        return true;
    static const std::set<String> without_if = {"min", "max", "any", "groupBitOr", "groupBitAnd", "groupBitXor"};
    return suffixes.empty() && without_if.contains(base);
}

/// Appends the tables of the join tree in order, and the ON expressions of its joins. Throws unless the join tree joins
/// tables by inner joins, whose delta is the UNION ALL of per-table terms.
void collectJoinTree(const QueryTreeNodePtr & node, QueryTreeNodes & tables, QueryTreeNodes & join_conditions)
{
    switch (node->getNodeType())
    {
        case QueryTreeNodeType::TABLE:
            tables.push_back(node);
            return;
        case QueryTreeNodeType::ARRAY_JOIN:
            collectJoinTree(node->as<ArrayJoinNode &>().getTableExpressionNode(), tables, join_conditions);
            return;
        case QueryTreeNodeType::CROSS_JOIN:
        {
            auto & join = node->as<CrossJoinNode &>();
            if (std::ranges::any_of(join.getJoinTypes(), [](const auto & type) { return type.locality == JoinLocality::Global; }))
                throwUnsupported("GLOBAL joins");
            for (const auto & table : join.getTableExpressions())
                collectJoinTree(table, tables, join_conditions);
            return;
        }
        case QueryTreeNodeType::JOIN:
        {
            /// Resolution has replaced an unspecified strictness by `join_default_strictness`.
            const auto & join = node->as<JoinNode &>();
            if (join.getKind() != JoinKind::Inner || join.getStrictness() != JoinStrictness::All)
                throwUnsupported("joins other than INNER ALL, CROSS and comma joins");
            if (join.getLocality() == JoinLocality::Global)
                throwUnsupported("GLOBAL joins");
            collectJoinTree(join.getLeftTableExpressionNode(), tables, join_conditions);
            collectJoinTree(join.getRightTableExpressionNode(), tables, join_conditions);
            if (join.isOnJoinExpression())
                join_conditions.push_back(join.getJoinExpression());
            return;
        }
        default:
            throwUnsupported("table functions or subqueries in FROM");
    }
}

/// Throws unless the table has block numbers that order its rows by insert, and no merges or mutations rewrite its rows.
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

    return {.alias = table.getAlias(), .key = toString(table_id.uuid), .storage = std::move(storage)};
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

/// Rejects tables outside FROM, like in `k IN t`, which the cursors do not cover. A linear query also rejects window
/// functions, whose result over the union of the terms differs from the union of their results.
void checkExpressions(const IQueryTreeNode & node, const InputIndexes & input_indexes, bool is_linear)
{
    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;
        auto type = child->getNodeType();
        if ((type == QueryTreeNodeType::TABLE || type == QueryTreeNodeType::TABLE_FUNCTION) && !input_indexes.contains(child.get()))
            throwUnsupported("IN over tables");
        if (const auto * function = child->as<FunctionNode>(); is_linear && function && function->isWindowFunction())
            throwUnsupported(fmt::format("window functions in {}", linear_queries));
        checkExpressions(*child, input_indexes, is_linear);
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

void splitConjuncts(const QueryTreeNodePtr & node, QueryTreeNodes & conjuncts)
{
    if (const auto * function = node->as<FunctionNode>(); function && function->getFunctionName() == "and")
    {
        for (const auto & argument : function->getArguments().getNodes())
            splitConjuncts(argument, conjuncts);
    }
    else
        conjuncts.push_back(node);
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
        edges.push_back({a_input->second, a->getColumnName(), b_input->second, b->getColumnName()});
        edges.push_back({b_input->second, b->getColumnName(), a_input->second, a->getColumnName()});
    }
    return edges;
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

/// `qualifier.column`, or `column` for an empty qualifier.
ASTPtr makeColumn(const String & qualifier, const String & column)
{
    if (qualifier.empty())
        return make_intrusive<ASTIdentifier>(column);
    return make_intrusive<ASTIdentifier>(std::vector<String>{qualifier, column});
}

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

/// Conjuncts that keep the rows of an input with `lower` <= `_block_number` < `upper`, where a null `lower` keeps every row
/// below `upper`. The conjuncts on `mergeTreePartInfo(_part)` depend only on the part, so they skip the parts outside the
/// bounds before reading them.
void addBounds(ASTs & conjuncts, const String & qualifier, const std::map<String, Int64> * lower,
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

/// Adds `SETTINGS name = value` to `select`, keeping its other settings.
void setSetting(ASTSelectQuery & select, std::string_view name, const Field & value)
{
    auto settings = select.settings() ? select.settings()->clone() : make_intrusive<ASTSetQuery>();
    auto & set_query = settings->as<ASTSetQuery &>();
    set_query.is_standalone = false;
    set_query.changes.setSetting(name, value);
    select.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));
}

/// Rewrites the resolved `select` of `query` to carry the weights of its rows, the product of the signs of the signed
/// inputs. A query without aggregates or GROUP BY returns the weight of each row as `sign_column`. A query with aggregates
/// weighs the rows in its aggregates, and is unchanged without signed inputs.
void addWeights(ASTSelectQuery & select, const QueryNode & query, const ASTs & signs, const String & sign_column)
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

ASTPtr makeSelectWithUnion(ASTPtr select)
{
    auto selects = make_intrusive<ASTExpressionList>();
    selects->children.push_back(std::move(select));
    auto query = make_intrusive<ASTSelectWithUnionQuery>();
    query->list_of_selects = selects;
    query->children.push_back(std::move(selects));
    return query;
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
    return make_intrusive<ASTSubquery>(makeSelectWithUnion(std::move(select)));
}

/// Whether the clauses of a query with `delta_query_sign_column` need the integral of the core. The core is the rest of the
/// query, which `checkLinearClauses` and `checkResultColumns` accept.
bool needsResidual(const QueryNode & query, bool is_signed)
{
    if (query.isDistinct() || query.hasHaving() || query.hasWindow() || query.hasQualify() || query.hasOrderBy() || query.hasInterpolate()
        || query.hasLimitBy() || query.hasLimit() || query.hasOffset() || query.hasLimitAfter() || query.hasLimitUntil())
        return true;
    for (const auto & column : query.getProjection().getNodes())
    {
        if (hasWindowFunctionNodes(column))
            return true;
        if (const auto * function = column->as<FunctionNode>(); function && function->isAggregateFunction())
        {
            const auto & name = function->getFunctionName();
            if (is_signed ? !isInvertibleAggregate(name) : !isMergeableAggregate(name))
                return true;
        }
        else if (hasAggregateFunctionNodes(column))
            return true;
    }
    return false;
}

/// The name of the table that stands for the merged rows of the integral in `Residual::outer`.
constexpr std::string_view residual_input = "__residual_input";

String makeKeyName(size_t key) { return fmt::format("__key_{}", key + 1); }

bool isAggregate(const IAST & ast)
{
    const auto * function = ast.as<ASTFunction>();
    return function && !function->isWindowFunction() && AggregateFunctionFactory::instance().isAggregateFunctionName(function->name);
}

using Visit = std::function<ASTPtr(const ASTPtr & node)>;

/// Calls `visit` on the nodes of the expression `node` of `parent` top-down. A node that `visit` returns replaces `node`,
/// and its children aren't visited. Neither are the identifiers of lambda parameters.
void visitExpression(ASTPtr & node, IAST & parent, const Visit & visit, std::multiset<String> & lambda_parameters)
{
    if (const auto * identifier = node->as<ASTIdentifier>(); identifier && lambda_parameters.contains(identifier->name()))
        return;
    if (auto replacement = visit(node))
    {
        const IAST * old = node.get();
        node = std::move(replacement);
        parent.updatePointerToChild(old, node);
        return;
    }
    if (auto * function = node->as<ASTFunction>(); function && function->isLambdaFunction())
    {
        auto & arguments = function->arguments->children;
        Names names;
        for (const auto & parameter : arguments.at(0)->as<ASTFunction &>().arguments->children)
            names.push_back(parameter->as<ASTIdentifier &>().name());
        for (const auto & name : names)
            lambda_parameters.insert(name);
        visitExpression(arguments.at(1), *function->arguments, visit, lambda_parameters);
        for (const auto & name : names)
            lambda_parameters.erase(lambda_parameters.find(name));
        return;
    }
    for (auto & child : node->children)
        if (child)
            visitExpression(child, *node, visit, lambda_parameters);
}

/// Visits the clauses of `select` that the residual evaluates.
void visitResidualClauses(ASTSelectQuery & select, const Visit & visit)
{
    std::multiset<String> lambda_parameters;
    for (const auto & list : {select.select(), select.window(), select.orderBy(), select.limitBy(), select.interpolate()})
        if (list)
            for (auto & child : list->children)
                visitExpression(child, *list, visit, lambda_parameters);
    if (select.having())
        visitExpression(select.refHaving(), select, visit, lambda_parameters);
    if (select.qualify())
        visitExpression(select.refQualify(), select, visit, lambda_parameters);
}

/// The PARTITION BY of the window of `function`, or null for a window over all rows.
ASTPtr getWindowPartition(const ASTFunction & function, const ASTSelectQuery & select)
{
    auto find_window = [&](const String & name) -> const ASTWindowDefinition &
    {
        if (select.window())
            for (const auto & element : select.window()->children)
                if (const auto & window = element->as<ASTWindowListElement &>(); window.name == name)
                    return window.definition->as<ASTWindowDefinition &>();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Window {} is not defined", name);
    };
    const auto * window = function.window_definition ? &function.window_definition->as<ASTWindowDefinition &>() : &find_window(function.window_name);
    while (!window->partition_by && !window->parent_window_name.empty())
        window = &find_window(window->parent_window_name);
    return window->partition_by;
}

ASTs cloneAll(const ASTs & asts)
{
    ASTs result;
    for (const auto & ast : asts)
        result.push_back(ast->clone());
    return result;
}

/// Splits the resolved `select` of `query` into its core, which it returns, and the residual, which it fills in.
///
/// The keys of the core are the GROUP BY keys, or else the columns that the residual reads. The core selects the keys as
/// `__key_i` and the aggregates of the integral as `__agg_m`. The residual is `select` with each key and aggregate replaced
/// by a column of its input, GROUP BY dropped and HAVING turned into WHERE. Its input selects the keys and merges the
/// aggregate columns of the integral per key as `__val_j`. Nodes match by their hashes without aliases.
ASTPtr splitResidual(const QueryNode & query, const ASTSelectQuery & select, bool is_signed, DeltaQuery::Residual & residual)
{
    if (query.isGroupByWithTotals() || query.isGroupByWithRollup() || query.isGroupByWithCube() || query.isGroupByWithGroupingSets())
        throwUnsupported(fmt::format("WITH TOTALS, ROLLUP, CUBE or GROUPING SETS in {}", linear_queries));
    for (const auto & column : query.getProjectionColumns())
    {
        if (column.name == residual.sign_column)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The delta query returns column {}, which delta_query_sign_column names", column.name);
        if (std::ranges::find(residual.outputs, column.name) != residual.outputs.end())
            throwUnsupported(fmt::format("two result columns named {} in queries with HAVING, ORDER BY, LIMIT, DISTINCT, windows or expressions over aggregates", column.name));
        residual.outputs.push_back(column.name);
    }

    auto outer_ast = select.clone();
    auto & outer = outer_ast->as<ASTSelectQuery &>();

    ASTs keys;
    std::map<IASTHash, size_t> key_indexes;
    auto add_key = [&](const ASTPtr & key)
    {
        if (key_indexes.try_emplace(key->getTreeHash(true), keys.size()).second)
            keys.push_back(key->clone());
    };
    residual.has_aggregates = query.hasGroupBy();
    visitResidualClauses(outer, [&](const ASTPtr & node) -> ASTPtr
    {
        residual.has_aggregates |= isAggregate(*node);
        return nullptr;
    });
    if (residual.has_aggregates)
    {
        for (const auto & key : query.getGroupBy().getNodes())
            add_key(key->toAST());
    }
    else
    {
        visitResidualClauses(outer, [&](const ASTPtr & node) -> ASTPtr
        {
            if (node->as<ASTIdentifier>())
                add_key(node);
            return nullptr;
        });
    }
    residual.num_keys = keys.size();

    /// The partition starts with all keys and keeps those that every clause that compares rows across keys compares by.
    std::vector<bool> in_partition(keys.size(), true);
    auto restrict_to = [&](const ASTPtr & list)
    {
        std::vector<bool> matched(keys.size());
        if (list)
            for (const auto & expression : list->children)
                if (auto key = key_indexes.find(expression->getTreeHash(true)); key != key_indexes.end())
                    matched[key->second] = true;
        for (size_t i = 0; i < keys.size(); ++i)
            in_partition[i] = in_partition[i] && matched[i];
    };
    bool with_fill = false;
    if (outer.orderBy())
        for (const auto & element : outer.orderBy()->children)
            with_fill |= element->as<ASTOrderByElement &>().with_fill;
    if (outer.limitLength() || outer.limitOffset() || outer.limitAfter() || outer.limitUntil() || outer.interpolate() || with_fill)
        restrict_to(nullptr);
    if (outer.limitBy())
        restrict_to(outer.limitBy());
    visitResidualClauses(outer, [&](const ASTPtr & node) -> ASTPtr
    {
        if (const auto * function = node->as<ASTFunction>(); function && function->isWindowFunction())
            restrict_to(getWindowPartition(*function, outer));
        return nullptr;
    });
    /// The rows of the target for a touched value are read by the output column of the key.
    restrict_to(outer.select());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (!in_partition[i])
            continue;
        const auto & projection = outer.select()->children;
        auto hash = keys[i]->getTreeHash(true);
        auto column = std::ranges::find_if(projection, [&](const auto & expression) { return expression->getTreeHash(true) == hash; });
        residual.partition_keys.push_back(i);
        residual.partition_outputs.push_back(residual.outputs.at(column - projection.begin()));
    }

    /// The aggregates of the core by their hashes, and the columns of the integral for them.
    ASTs core_aggregates;
    std::map<IASTHash, String> core_aggregate_columns;
    auto add_core_aggregate = [&](ASTPtr aggregate, const String & simple_function) -> ASTPtr
    {
        auto [column, inserted] = core_aggregate_columns.try_emplace(aggregate->getTreeHash(true), fmt::format("__agg_{}", core_aggregates.size()));
        if (inserted)
        {
            aggregate->setAlias(column->second);
            core_aggregates.push_back(std::move(aggregate));
            residual.simple_functions.push_back(simple_function);
        }
        return make_intrusive<ASTIdentifier>(column->second);
    };
    add_core_aggregate(makeASTFunction("count"), "sum");

    /// `name(arguments)` with the parameters of `like`.
    auto make_aggregate = [](const String & name, const ASTFunction & like, ASTs arguments)
    {
        auto function = makeASTFunction(name, std::move(arguments));
        if (like.parameters)
        {
            function->parameters = like.parameters->clone();
            function->children.push_back(function->parameters);
        }
        return function;
    };
    /// Adds the aggregates of the core that `function` merges from, and returns the merge.
    auto add_aggregate = [&](const ASTFunction & function) -> ASTPtr
    {
        const auto [base, suffixes] = splitCombinators(function.name);
        const ASTs arguments = function.arguments ? function.arguments->children : ASTs{};
        const bool is_sum = suffixes.empty() || suffixes == std::vector<String>{"If"};
        if (!suffixes.empty() && suffixes.front() == "State")
        {
            if (is_signed && !isInvertibleAggregate(function.name))
                throwUnsupported(fmt::format("aggregate function {} over signed inputs", function.name));
            auto column = add_core_aggregate(function.clone(), "");
            return make_aggregate(function.name.substr(0, function.name.size() - String("State").size()) + "MergeState", function, {column});
        }
        if ((base == "sum" || base == "count") && is_sum)
            return makeASTFunction("sum", add_core_aggregate(function.clone(), "sum"));
        if (base == "avg" && is_sum)
        {
            /// `avg(x)` is `sum(x) / count(x)`, which sums over rows.
            const String suffix = suffixes.empty() ? "" : "If";
            auto sum = add_core_aggregate(makeASTFunction("sum" + suffix, cloneAll(arguments)), "sum");
            auto count = add_core_aggregate(makeASTFunction("count" + suffix, cloneAll(arguments)), "sum");
            return makeASTFunction("divide", makeASTFunction("sum", std::move(sum)), makeASTFunction("sum", std::move(count)));
        }
        if (is_signed)
            throwUnsupported(fmt::format("aggregate function {} over signed inputs, use count, sum, avg, their -If forms or exponentialMovingAverageState instead", function.name));
        static const std::set<String> simple = {"min", "max", "any", "groupBitOr", "groupBitAnd", "groupBitXor"};
        if (suffixes.empty() && simple.contains(base))
            return makeASTFunction(base, add_core_aggregate(function.clone(), base));
        auto column = add_core_aggregate(make_aggregate(function.name + "State", function, cloneAll(arguments)), "");
        return make_aggregate(function.name + "Merge", function, {column});
    };

    std::unordered_set<const IAST *> replacements;
    auto replace_by = [&](const String & name, const ASTPtr & node) -> ASTPtr
    {
        auto identifier = make_intrusive<ASTIdentifier>(name);
        identifier->setAlias(node->tryGetAlias());
        replacements.insert(identifier.get());
        return identifier;
    };
    std::map<IASTHash, size_t> value_indexes;
    visitResidualClauses(outer, [&](const ASTPtr & node) -> ASTPtr
    {
        auto hash = node->getTreeHash(true);
        if (auto key = key_indexes.find(hash); key != key_indexes.end())
            return replace_by(makeKeyName(key->second), node);
        if (!isAggregate(*node))
            return nullptr;
        auto [value, inserted] = value_indexes.try_emplace(hash, residual.merges.size());
        if (inserted)
            residual.merges.push_back(add_aggregate(node->as<ASTFunction &>()));
        return replace_by(fmt::format("__val_{}", value->second + 1), node);
    });
    visitResidualClauses(outer, [&](const ASTPtr & node) -> ASTPtr
    {
        if (node->as<ASTIdentifier>() && !replacements.contains(node.get()))
            throwUnsupported(fmt::format("{} outside aggregates and GROUP BY keys", node->formatForErrorMessage()));
        return nullptr;
    });

    auto having = outer.having();
    outer.setExpression(ASTSelectQuery::Expression::HAVING, nullptr);
    outer.setExpression(ASTSelectQuery::Expression::WHERE, std::move(having));
    for (auto clause : {ASTSelectQuery::Expression::WITH, ASTSelectQuery::Expression::PREWHERE, ASTSelectQuery::Expression::GROUP_BY,
            ASTSelectQuery::Expression::SETTINGS, ASTSelectQuery::Expression::TABLES})
        outer.setExpression(clause, nullptr);
    outer.group_by_all = false;
    outer.replaceDatabaseAndTable(StorageID("", String(residual_input)));
    for (size_t i = 0; i < residual.outputs.size(); ++i)
        outer.select()->children[i]->setAlias(residual.outputs[i]);
    residual.outer = outer_ast->formatWithSecretsOneLine();

    auto core_ast = select.clone();
    auto & core = core_ast->as<ASTSelectQuery &>();
    auto projection = make_intrusive<ASTExpressionList>();
    auto group_by = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < keys.size(); ++i)
    {
        auto key = keys[i]->clone();
        key->setAlias("");
        group_by->children.push_back(key->clone());
        key->setAlias(makeKeyName(i));
        projection->children.push_back(std::move(key));
    }
    std::ranges::move(core_aggregates, std::back_inserter(projection->children));
    core.setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection));
    core.setExpression(ASTSelectQuery::Expression::GROUP_BY, nullptr);
    if (!keys.empty())
        core.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by));
    for (auto clause : {ASTSelectQuery::Expression::HAVING, ASTSelectQuery::Expression::WINDOW, ASTSelectQuery::Expression::QUALIFY,
            ASTSelectQuery::Expression::ORDER_BY, ASTSelectQuery::Expression::INTERPOLATE, ASTSelectQuery::Expression::LIMIT_BY_OFFSET,
            ASTSelectQuery::Expression::LIMIT_BY_LENGTH, ASTSelectQuery::Expression::LIMIT_BY, ASTSelectQuery::Expression::LIMIT_OFFSET,
            ASTSelectQuery::Expression::LIMIT_LENGTH, ASTSelectQuery::Expression::LIMIT_AFTER, ASTSelectQuery::Expression::LIMIT_UNTIL})
        core.setExpression(clause, nullptr);
    core.distinct = false;
    core.group_by_all = false;
    core.limit_with_ties = false;
    core.limit_by_all = false;
    core.limit_after_all = false;
    return makeSelectWithUnion(std::move(core_ast));
}

String joinNames(const Strings & names)
{
    return fmt::format("{}", fmt::join(names, ", "));
}

/// Parses `sql`, whose unions have explicit modes.
ASTPtr parseSelect(const String & sql)
{
    ParserSelectWithUnionQuery parser;
    auto ast = parseQuery(parser, sql, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    NormalizeSelectWithUnionQueryVisitor::Data data{SetOperationMode::ALL};
    NormalizeSelectWithUnionQueryVisitor(data).visit(ast);
    return ast;
}

/// Replaces the identifiers of the columns in `types` within `ast` by casts to their types.
void castColumns(IAST & ast, const std::map<String, String> & types)
{
    for (auto & child : ast.children)
    {
        const auto * identifier = child->as<ASTIdentifier>();
        if (!identifier)
            castColumns(*child, types);
        else if (auto type = types.find(identifier->name()); type != types.end())
            child = makeASTFunction("CAST", child, make_intrusive<ASTLiteral>(type->second));
    }
}

}

DeltaQuery::DeltaQuery(const ASTPtr & query_, const ContextPtr & context_)
{
    checkNoSubqueries(*query_);
    /// The query can set `delta_query_sign_column` in its SETTINGS, as the query of a view does.
    auto context = Context::createCopy(context_);
    InterpreterSetQuery::applySettingsFromQuery(query_, context);
    const String sign_column = context->getSettingsRef()[Setting::delta_query_sign_column];

    Residual split;
    split.sign_column = sign_column;
    auto core = analyze(query_, context, sign_column, sign_column.empty() ? nullptr : &split);
    if (!core)
        return;
    analyze(core, context, sign_column, nullptr);

    /// The integral has the columns of the deltas of the core, and merges the aggregate columns.
    const auto core_columns = InterpreterSelectQueryAnalyzer::getSampleBlock(query, context)->getNamesAndTypesList();
    Strings structure;
    /// The merges of a `SimpleAggregateFunction` column would have its type, so they read the values of the column.
    std::map<String, String> simple_types;
    size_t index = 0;
    for (const auto & column : core_columns)
    {
        auto type = column.type;
        if (index < split.num_keys)
            split.key_types.push_back(type);
        else if (const auto & function = split.simple_functions.at(index - split.num_keys); !function.empty())
        {
            const auto value_type = removeLowCardinality(type)->getName();
            simple_types.emplace(column.name, value_type);
            type = DataTypeFactory::instance().get(fmt::format("SimpleAggregateFunction({}, {})", function, value_type));
        }
        split.integral_columns.emplace_back(column.name, type);
        structure.push_back(fmt::format("{} {}", backQuote(column.name), type->getName()));
        ++index;
    }
    for (size_t i = 0; i < split.merges.size(); ++i)
    {
        castColumns(*split.merges[i], simple_types);
        split.values.push_back(fmt::format("{} AS {}", split.merges[i]->formatWithSecretsOneLine(), backQuote(fmt::format("__val_{}", i + 1))));
    }

    /// The residual over an empty integral has the types of the rows that the residual deltas insert.
    split.output_query = parseSelect(fmt::format("SELECT *, toInt8(1) AS {} FROM ({})", backQuote(sign_column),
        split.buildResidual(fmt::format("null({})", quoteString(joinNames(structure))), "")));
    for (const auto & column : *InterpreterSelectQueryAnalyzer::getSampleBlock(split.output_query, context))
        if (WhichDataType(column.type).isAggregateFunction())
            throwUnsupported(fmt::format("aggregate states like column {} in the results of queries with HAVING, ORDER BY, LIMIT, DISTINCT, windows or expressions over aggregates", column.name));
    residual = std::make_shared<const Residual>(std::move(split));
}

ASTPtr DeltaQuery::analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column, Residual * split)
{
    inputs.clear();
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
    collectJoinTree(query_node->getJoinTreeNode(), tables, conditions);
    InputIndexes input_indexes;
    for (const auto & table : tables)
    {
        input_indexes.emplace(table.get(), inputs.size());
        inputs.push_back(makeInput(table->as<TableNode &>()));
    }

    /// The deltas of a join or of a query with signed inputs must merge into the query over the union of the inputs. The
    /// delta of any other query is the query over the new rows of its table.
    const bool is_join = inputs.size() > 1;
    const bool is_linear = is_join || !sign_column.empty();
    ASTs signs;
    std::unordered_set<const IQueryTreeNode *> signed_tables;
    for (size_t i = 0; i < tables.size() && !sign_column.empty(); ++i)
    {
        if (!hasSignColumn(tables[i]->as<TableNode &>(), sign_column))
            continue;
        signs.push_back(makeColumn(inputs[i].alias, sign_column));
        signed_tables.insert(tables[i].get());
    }
    checkSignColumnNotRead(*query_tree, signed_tables, sign_column);
    const bool has_residual = split && needsResidual(*query_node, !signs.empty());
    checkExpressions(*query_tree, input_indexes, is_linear && !has_residual);
    if (has_residual)
    {
        auto resolved = query_tree->toAST();
        return splitResidual(*query_node, resolved->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>(),
            !signs.empty(), *split);
    }
    if (is_linear)
    {
        checkLinearClauses(*query_node);
        checkResultColumns(*query_node, !signs.empty());
    }

    if (query_node->hasWhere())
        conditions.push_back(query_node->getWhere());
    semi_join_edges = collectSemiJoinEdges(conditions, input_indexes);
    query = query_tree->toAST();

    auto & select = query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
    /// Without GROUP BY, a delta that reads no rows still returns one row by default, with 0 for `min` and `max`.
    if (is_linear && !select.groupBy())
        setSetting(select, "empty_result_for_aggregation_by_empty_set", true);
    if (!sign_column.empty())
        addWeights(select, *query_node, signs, sign_column);

    /// Resolution checks only that the user can see the tables. Building the plan checks the grants on the columns that
    /// the query reads.
    interpreter.getSampleBlock();
    return nullptr;
}

DeltaQuery::~DeltaQuery() = default;

ASTPtr DeltaQuery::getQuery() const
{
    return residual ? residual->output_query : query;
}

String DeltaQuery::Residual::buildInput(const String & integral, const String & where) const
{
    Strings keys;
    for (size_t i = 0; i < num_keys; ++i)
        keys.push_back(backQuote(makeKeyName(i)));
    const String group_by = keys.empty() ? "" : " GROUP BY " + joinNames(keys);
    auto columns = keys;
    if (!has_aggregates)
    {
        /// A row of the integral with count n stands for n rows.
        columns.push_back("sum(`__agg_0`) AS `__count`");
        auto expanded = keys;
        expanded.push_back("`__index`");
        return fmt::format("SELECT {} FROM (SELECT {} FROM {}{}{} HAVING `__count` > 0) ARRAY JOIN range(toUInt64(`__count`)) AS `__index`",
            joinNames(expanded), joinNames(columns), integral, where, group_by);
    }
    /// A group whose rows were all retracted has count 0. A query without GROUP BY returns its one row anyway.
    columns.insert(columns.end(), values.begin(), values.end());
    columns.push_back("sum(`__agg_0`) AS `__count`");
    return fmt::format("SELECT {} FROM {}{}{}{}", joinNames(columns), integral, where, group_by, keys.empty() ? "" : " HAVING `__count` > 0");
}

String DeltaQuery::Residual::buildResidual(const String & integral, const String & where) const
{
    auto result = outer;
    boost::replace_first(result, fmt::format("FROM {}", residual_input), fmt::format("FROM ({})", buildInput(integral, where)));
    return result;
}

String DeltaQuery::Residual::buildPartitionFilter(const Names & columns, const String & delta) const
{
    /// `IN` drops NULLs, so a nullable key compares as whether it is NULL and its value otherwise.
    Strings left;
    Strings right;
    for (size_t i = 0; i < partition_keys.size(); ++i)
    {
        auto column = backQuote(columns.at(i));
        auto key = backQuote(makeKeyName(partition_keys[i]));
        if (isNullableOrLowCardinalityNullable(key_types.at(partition_keys[i])))
        {
            left.insert(left.end(), {fmt::format("isNull({})", column), fmt::format("assumeNotNull({})", column)});
            right.insert(right.end(), {fmt::format("isNull({})", key), fmt::format("assumeNotNull({})", key)});
        }
        else
        {
            left.push_back(column);
            right.push_back(key);
        }
    }
    return fmt::format("({}) IN (SELECT {} FROM {})", joinNames(left), joinNames(right), delta);
}

String DeltaQuery::getIntegralCreateQuery(const StorageID & integral) const
{
    chassert(residual);
    Strings columns;
    for (const auto & column : residual->integral_columns)
        columns.push_back(fmt::format("{} {}", backQuote(column.name), column.type->getName()));
    /// The keys of the partition come first, so that the filter by touched values reads ranges of the primary key.
    Strings order;
    for (auto key : residual->partition_keys)
        order.push_back(backQuote(makeKeyName(key)));
    for (size_t i = 0; i < residual->num_keys; ++i)
        if (std::ranges::find(residual->partition_keys, i) == residual->partition_keys.end())
            order.push_back(backQuote(makeKeyName(i)));
    return fmt::format("CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = AggregatingMergeTree ORDER BY {} SETTINGS allow_nullable_key = 1",
        integral.getFullTableName(), joinNames(columns), order.empty() ? "tuple()" : "(" + joinNames(order) + ")");
}

/// The residual over the touched keys counts each of its rows once, and the target counts each of its rows of those keys by
/// the sum of their signs. The difference of the counts per row is the delta, which becomes that many rows of sign 1 or -1.
ASTPtr DeltaQuery::buildResidualDelta(const StorageID & integral, const StorageID & delta, bool delta_is_empty, const StorageID & target) const
{
    chassert(residual);
    String where;
    String target_where;
    if (!residual->partition_keys.empty())
    {
        Names keys;
        for (auto key : residual->partition_keys)
            keys.push_back(makeKeyName(key));
        where = " WHERE " + residual->buildPartitionFilter(keys, delta.getFullTableName());
        target_where = " WHERE " + residual->buildPartitionFilter(residual->partition_outputs, delta.getFullTableName());
    }
    Strings outputs;
    for (const auto & output : residual->outputs)
        outputs.push_back(backQuote(output));
    const auto columns = joinNames(outputs);
    const auto sign = backQuote(residual->sign_column);
    return parseSelect(fmt::format(
        "SELECT {0}, arrayJoin(arrayWithConstant(toUInt64(abs(`__count`)), toInt8(sign(`__count`)))) AS {1} "
        "FROM (SELECT {0}, sum(`__weight`) AS `__count` "
        "FROM (SELECT {0}, toInt64(1) AS `__weight` FROM ({2}) UNION ALL SELECT {0}, -toInt64({1}) AS `__weight` FROM {3}{4}) "
        "GROUP BY {0} HAVING `__count` != 0){5}",
        columns, sign, residual->buildResidual(integral.getFullTableName(), where), target.getFullTableName(), target_where,
        delta_is_empty ? " WHERE 0" : ""));
}

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

    const auto & select = query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
    auto terms = make_intrusive<ASTExpressionList>();
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        auto add_bounds = [&](ASTs & conjuncts, size_t j, const String & qualifier)
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
            add_bounds(conjuncts, j, inputs[j].alias);

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
            for (const auto & [a, x, b, y] : semi_join_edges)
            {
                if (a != from || reached[b])
                    continue;
                reached[b] = true;
                queue.push_back(b);

                ASTs where;
                add_bounds(where, a, "");
                if (reductions[a])
                    where.push_back(reductions[a]->clone());
                auto subquery = makeSemiJoinSubquery(inputs[a], x, makeASTFunction("and", std::move(where)));
                reductions[b] = makeASTFunction("in", make_intrusive<ASTIdentifier>(y), subquery->clone());
                conjuncts.push_back(makeASTFunction("in", makeColumn(inputs[b].alias, y), std::move(subquery)));
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
