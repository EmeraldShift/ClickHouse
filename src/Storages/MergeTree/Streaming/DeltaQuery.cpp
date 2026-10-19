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
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageProxy.h>

#include <base/find_symbols.h>
#include <boost/algorithm/string/trim.hpp>
#include <chrono>
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
    extern const int DELTA_QUERY_HISTORY_LOST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNFINISHED;
}

namespace Setting
{
    extern const SettingsString delta_query_lookup_tables;
    extern const SettingsString delta_query_recursion_table;
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
    /// Whether `delta_query_lookup_tables` names the table. Its new rows then have no term, and the other terms read its
    /// rows below the new cursor.
    bool lookup = false;
    bool is_signed = false;
    /// Whether lightweight updates can change the rows of the table. Delta queries read them from plain `MergeTree` tables,
    /// which have them with `enable_block_offset_column`.
    bool updatable = false;
    /// The columns of the table in the equalities of the ON clause, if the table is the right side of a LEFT, SEMI, ANTI or
    /// ASOF join. Such a join isn't linear in the table. A new row of the table changes the result for every left row of
    /// its join keys, see `buildDelta`.
    Names join_keys = {};
    /// The columns of such a table that the query reads.
    Names columns = {};
};

/// An equality `from.from_column = to.to_column` between join keys of two inputs. The semi-join reduction follows it from
/// input `from` to input `to`.
struct DeltaQuery::SemiJoinEdge
{
    size_t from;
    String from_column;
    size_t to;
    String to_column;

    bool operator==(const SemiJoinEdge &) const = default;
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
    /// The merges of `sum` and `avg`, with the integral columns of their sum and of their count of non-NULL arguments. The
    /// count is null for a `sum`, whose integral has no count unless its argument is Nullable.
    struct SumMerge
    {
        size_t merge;
        String sum_column;
        String count_column;
        ASTPtr count;
    };
    std::vector<SumMerge> sum_merges;
    /// The residual as a select without FROM.
    ASTPtr outer;
    Names outputs;
    /// The keys whose values bound the rows that a change of the integral can change, and the output columns that equal
    /// them. A refresh recomputes the rows of the touched values of these keys. Empty if it recomputes all rows.
    std::vector<size_t> partition_keys;
    Names partition_outputs;
    /// The output column that equals each key, or empty.
    Names key_outputs;
    /// Whether the residual over its own result and new rows equals the residual over all rows. Then the target holds all
    /// the state and the view needs no integral. That holds over unsigned inputs, which only get inserts, for a residual
    /// over rows that only drops rows that inserts can't bring back, like LIMIT, LIMIT BY, DISTINCT and QUALIFY with an
    /// upper bound on a rank. Every key must be an output column, so the target rows can stand for the input rows.
    bool append_only = false;

    /// Filled from the analysis of the core.
    DataTypes key_types;
    NamesAndTypesList integral_columns;
    /// `merge AS __val_j` for each merge.
    Strings values;
    ASTPtr output_query;

    /// The rows of `integral` that pass `where` merged per key.
    String buildInput(const String & integral, const String & where) const;
    /// The net rows of `target` that pass `where` as rows of keys, and the rows of the delta of the core in `delta`.
    String buildAppendOnlyInput(const String & target, const String & where, const String & delta) const;
    /// The residual over `input`, which selects the keys and the merged aggregates.
    String buildResidual(const String & input) const;
    /// `(c_1, ...) IN (SELECT __key_p1, ... FROM delta)`, where column c_i equals partition key i.
    String buildPartitionFilter(const Names & columns, const String & delta) const;
};

/// The base and the steps of a query `WITH RECURSIVE r AS (base UNION ALL step ...) SELECT * FROM r`. Each step reads the
/// table of `delta_query_recursion_table` instead of `r`, as a lookup table. Its delta then joins the new rows of its other
/// inputs with the rows of `r` at the old cursor.
struct DeltaQuery::Recursion
{
    std::unique_ptr<DeltaQuery> base;
    /// Empty without `delta_query_recursion_table`. Such a query can only give the columns of its deltas.
    std::vector<std::unique_ptr<DeltaQuery>> steps;
    /// The key of the table of `delta_query_recursion_table`.
    String key;

    void checkTable() const
    {
        if (steps.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Delta queries over WITH RECURSIVE need delta_query_recursion_table, the table that holds their result at the old cursor");
    }
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
            if (is_keyed && (join.getRightTableExpressionNode()->getNodeType() != QueryTreeNodeType::TABLE || !join.isOnJoinExpression()))
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

/// The table identifier, table function or subquery of `expression`.
ASTPtr & getTable(ASTTableExpression & expression)
{
    if (expression.table_function)
        return expression.table_function;
    if (expression.subquery)
        return expression.subquery;
    return expression.database_and_table_name;
}

/// The table expression in FROM of `select` whose table passes `matches`, or null.
ASTTableExpression * findTable(const ASTSelectQuery & select, const std::function<bool(const IAST & table)> & matches)
{
    if (!select.tables())
        return nullptr;
    for (const auto & element : select.tables()->children)
    {
        const auto & table_expression = element->as<ASTTablesInSelectQueryElement &>().table_expression;
        if (!table_expression)
            continue;
        auto & expression = table_expression->as<ASTTableExpression &>();
        if (const auto & table = getTable(expression); table && matches(*table))
            return &expression;
    }
    return nullptr;
}

/// The table expression in FROM of `select` of the table with alias `alias`.
ASTTableExpression & findAliasedTable(const ASTSelectQuery & select, const String & alias)
{
    auto * expression = findTable(select, [&](const IAST & table) { return table.tryGetAlias() == alias; });
    chassert(expression);
    return *expression;
}

/// Replaces the table of `expression` by `table`, a table identifier, a table function or a subquery.
void setTable(ASTTableExpression & expression, ASTPtr table)
{
    expression.reset(getTable(expression));
    auto & field = table->as<ASTTableIdentifier>() ? expression.database_and_table_name
        : table->as<ASTFunction>() ? expression.table_function : expression.subquery;
    expression.setOrReplace(field, std::move(table));
}

auto isTableNamed(const String & name)
{
    return [name](const IAST & table)
    {
        const auto * identifier = table.as<ASTTableIdentifier>();
        return identifier && identifier->name() == name;
    };
}

/// The number of table expressions in `ast` that read table `name`.
size_t countTableReferences(const IAST & ast, const String & name)
{
    const auto * expression = ast.as<ASTTableExpression>();
    size_t count = expression && expression->database_and_table_name && isTableNamed(name)(*expression->database_and_table_name) ? 1 : 0;
    for (const auto & child : ast.children)
        count += countTableReferences(*child, name);
    return count;
}

/// `query` with each `Distributed` table in FROM replaced by its local table, under the name of the `Distributed` table
/// unless it has an alias. The other tables get the current database, because a shard resolves tables in its own default
/// database. Returns null if `query` reads no `Distributed` table. Sets `cluster` to the cluster of the `Distributed`
/// tables, which must be the same.
ASTPtr readLocalTables(const ASTPtr & query, const ContextPtr & context, String & cluster)
{
    auto result = query->clone();
    for (const auto & select : result->as<ASTSelectWithUnionQuery &>().list_of_selects->children)
    {
        /// The analysis rejects UNION.
        const auto * select_query = select->as<ASTSelectQuery>();
        const auto & tables = select_query ? select_query->tables() : nullptr;
        if (!tables)
            continue;
        for (const auto & element : tables->children)
        {
            const auto & table_expression = element->as<ASTTablesInSelectQueryElement &>().table_expression;
            auto * expression = table_expression ? table_expression->as<ASTTableExpression>() : nullptr;
            const auto * identifier = expression && expression->database_and_table_name
                ? expression->database_and_table_name->as<ASTTableIdentifier>() : nullptr;
            if (!identifier)
                continue;
            const auto table_id = context->resolveStorageID(identifier->getTableId());
            auto storage = DatabaseCatalog::instance().tryGetTable(table_id, context);
            if (!storage)
                continue;
            ASTPtr table;
            auto alias = identifier->tryGetAlias();
            if (auto distributed = castStorage<StorageDistributed>(storage, DeferredTable::Load))
            {
                if (!cluster.empty() && cluster != distributed->getCluster()->getName())
                    throwUnsupported("Distributed tables of different clusters");
                cluster = distributed->getCluster()->getName();
                table = make_intrusive<ASTTableIdentifier>(distributed->getRemoteDatabaseName(), distributed->getRemoteTableName());
                if (alias.empty())
                    alias = table_id.table_name;
            }
            else
                table = make_intrusive<ASTTableIdentifier>(table_id.database_name, table_id.table_name);
            table->setAlias(alias);
            setTable(*expression, std::move(table));
        }
    }
    return cluster.empty() ? nullptr : result;
}

bool isAggregate(const IAST & ast);

bool hasAggregates(const IAST & ast)
{
    return isAggregate(ast) || std::ranges::any_of(ast.children, [](const auto & child) { return hasAggregates(*child); });
}

/// A query `WITH RECURSIVE r AS (base UNION ALL step ...) SELECT * FROM r`. The base doesn't read `r`, and each step reads
/// `r` once in FROM. The target then holds `r`, which the deltas of the steps read.
struct RecursiveCTE
{
    String name;
    /// The base and the steps.
    ASTs parts;
};

/// The recursive CTE of `query`, or nothing for a query without WITH RECURSIVE.
std::optional<RecursiveCTE> getRecursiveCTE(const IAST & query)
{
    const auto & selects = query.as<ASTSelectWithUnionQuery &>().list_of_selects->children;
    const auto * select = selects.size() == 1 ? selects[0]->as<ASTSelectQuery>() : nullptr;
    if (!select || !select->recursive_with)
        return {};
    const auto * cte = select->with()->children.size() == 1 ? select->with()->children[0]->as<ASTWithElement>() : nullptr;
    const bool selects_all = select->select()->children.size() == 1 && select->select()->children[0]->as<ASTAsterisk>();
    /// The definition of a view reads the CTE through a subquery with the name of the CTE.
    const auto * from = select->tables() && select->tables()->children.size() == 1
        ? select->tables()->children[0]->as<ASTTablesInSelectQueryElement &>().table_expression->as<ASTTableExpression>() : nullptr;
    const bool reads_cte = cte && from
        && ((from->database_and_table_name && isTableNamed(cte->name)(*from->database_and_table_name))
            || (from->subquery && from->subquery->as<ASTSubquery &>().cte_name == cte->name));
    if (!cte || cte->aliases || !selects_all || !reads_cte || select->distinct || select->where() || select->prewhere()
        || select->groupBy() || select->having() || select->orderBy() || select->limitBy() || select->limitLength()
        || select->limitOffset() || select->window() || select->qualify())
        throwUnsupported("WITH RECURSIVE except as WITH RECURSIVE r AS (...) SELECT * FROM r");

    const auto & body = cte->subquery->children.at(0)->as<ASTSelectWithUnionQuery &>();
    const auto & modes = body.list_of_modes;
    const bool union_all = modes.empty() ? body.union_mode == SelectUnionMode::UNION_ALL
        : std::ranges::all_of(modes, [](auto mode) { return mode == SelectUnionMode::UNION_ALL; });
    RecursiveCTE result{.name = cte->name, .parts = {}};
    for (const auto & child : body.list_of_selects->children)
    {
        /// The parser wraps each SELECT of the UNION in a UNION of one SELECT.
        const auto * part = child.get();
        if (const auto * inner = part->as<ASTSelectWithUnionQuery>(); inner && inner->list_of_selects->children.size() == 1)
            part = inner->list_of_selects->children[0].get();
        if (!part->as<ASTSelectQuery>())
            throwUnsupported("recursive CTEs other than a UNION ALL of SELECTs");
        result.parts.push_back(part->clone());
    }
    if (!union_all || result.parts.size() < 2)
        throwUnsupported("recursive CTEs other than a UNION ALL of SELECTs");
    for (size_t i = 0; i < result.parts.size(); ++i)
    {
        const auto & part = result.parts[i]->as<ASTSelectQuery &>();
        if (i == 0 && countTableReferences(part, cte->name) != 0)
            throwUnsupported("recursive CTEs whose first SELECT reads the CTE");
        if (i > 0 && (countTableReferences(part, cte->name) != 1 || !findTable(part, isTableNamed(cte->name))))
            throwUnsupported("recursive CTEs with a step that doesn't read the CTE exactly once in FROM");
        if (part.groupBy() || hasAggregates(*part.select()))
            throwUnsupported("aggregates in recursive CTEs");
    }
    return result;
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

Int64 getBound(const std::map<String, Int64> & partitions, const String & partition_id)
{
    auto it = partitions.find(partition_id);
    return it == partitions.end() ? 0 : it->second;
}

/// The parts of one table on this server, the promoters that report the blocks in flight to it, and its unfinished
/// mutations.
class TableBlocks
{
public:
    explicit TableBlocks(MergeTreeData & storage_)
        : storage(storage_)
        , parts(storage.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active},
            {MergeTreeData::DataPartKind::Regular, MergeTreeData::DataPartKind::Patch}))
        , blocks(getLocalParts(parts))
    {
        DB::checkReadableAsOf(storage, parts, {});
        /// Built after the parts snapshot, the promoters report a block committed in between as in flight.
        promoters = storage.buildPromoters();
        for (const auto & status : storage.getMutationsStatus())
            if (!status.is_done)
                for (const auto & [partition_id, block_number] : status.block_numbers)
                    unfinished_mutations.push_back({status.id, partition_id, block_number});
    }

    /// The block ranges of each partition, see `getLocalParts`.
    const LocalPartsByPartition & getParts() const { return blocks; }

    /// Whether this server has no parts of the partition and no parts in flight to it, as after the partition is dropped.
    bool isAbsent(const String & partition_id) const { return !blocks.contains(partition_id) && !promoters.contains(partition_id); }

    /// The first block number from `start` on that this server has not committed to the partition.
    Int64 getCommittedEnd(const String & partition_id, Int64 start) const
    {
        auto partition_blocks = blocks.find(partition_id);
        auto promoter = promoters.find(partition_id);
        if (partition_blocks == blocks.end() || promoter == promoters.end())
            return start;
        return promoteBlockNumber(start - 1, partition_blocks->second, promoter->second) + 1;
    }

    /// Whether some partition has rows with `lower` <= `_block_number` < `upper`, or for `updates`, lightweight updates
    /// with versions in that range. A patch part spans no bound that passes `checkReadableAsOf`.
    bool hasBlocks(const std::map<String, Int64> & lower, const std::map<String, Int64> & upper, bool updates) const
    {
        return std::ranges::any_of(parts, [&](const auto & part)
        {
            const auto range = getBlockRange(*part);
            return part->info.isPatch() == updates && range.max_block >= getBound(lower, range.getPartitionId())
                && range.min_block < getBound(upper, range.getPartitionId());
        });
    }

    /// Throws unless the parts hold the rows of the table as of `bounds` and the mutations below `bounds` are done. A mutation
    /// of a plain `MergeTree` table has one block number for every partition, under the empty partition ID.
    void checkReadableAsOf(const std::map<String, Int64> & bounds) const
    {
        DB::checkReadableAsOf(storage, parts, bounds);
        for (const auto & [id, partition_id, block_number] : unfinished_mutations)
            for (const auto & [bound_partition_id, bound] : bounds)
                if ((partition_id.empty() || partition_id == bound_partition_id) && block_number < bound)
                    throw Exception(ErrorCodes::UNFINISHED, "Delta queries wait for mutation {} of table {}, which is below block {} of partition {}",
                        id, storage.getStorageID().getNameForLogs(), bound, bound_partition_id);
    }

private:
    struct Mutation
    {
        String id;
        String partition_id;
        Int64 block_number;
    };

    MergeTreeData & storage;
    DataPartsVector parts;
    LocalPartsByPartition blocks;
    CursorPromotersMap promoters;
    std::vector<Mutation> unfinished_mutations;
};

std::map<String, TableBlocks> snapshotTables(const std::vector<Input> & inputs)
{
    std::map<String, TableBlocks> tables;
    for (const auto & input : inputs)
        tables.try_emplace(input.key, *input.storage);
    return tables;
}

/// The key of the time of the snapshot in the cursors of queries that call now(). Its value is in seconds.
constexpr auto time_key = "now";

std::optional<Int64> getTime(const CursorTreeNodePtr & tree)
{
    if (!tree || !tree->hasValue(time_key))
        return {};
    return tree->getValue(time_key);
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
        if (key == time_key && std::holds_alternative<Int64>(value))
            continue;
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

/// `c_1 AND ...`, or `1` without conjuncts.
ASTPtr makeConjunction(ASTs conjuncts)
{
    if (conjuncts.empty())
        return make_intrusive<ASTLiteral>(Field(UInt8{1}));
    if (conjuncts.size() == 1)
        return std::move(conjuncts[0]);
    return makeASTFunction("and", std::move(conjuncts));
}

ASTs cloneAll(const ASTs & asts)
{
    ASTs result;
    for (const auto & ast : asts)
        result.push_back(ast->clone());
    return result;
}

/// The factor of the weights of queries whose deltas have terms that retract rows, see `DeltaQuery::buildDelta`.
constexpr auto term_sign_name = "__term_sign";

/// Replaces the identifier `__term_sign` by `sign` and the calls of now() by `time`.
void bindTerm(IAST & ast, std::optional<Int64> sign, std::optional<Int64> time)
{
    for (auto & child : ast.children)
    {
        const auto * identifier = child->as<ASTIdentifier>();
        const auto * function = child->as<ASTFunction>();
        if (sign && identifier && identifier->name() == term_sign_name)
            child = make_intrusive<ASTLiteral>(Field(*sign));
        else if (time && function && function->name == "now")
            child = makeASTFunction("toDateTime", make_intrusive<ASTLiteral>(Field(static_cast<UInt64>(*time))));
        else
            bindTerm(*child, sign, time);
    }
}

bool callsNow(const IAST & ast)
{
    if (const auto * function = ast.as<ASTFunction>(); function && function->name == "now")
        return true;
    return std::ranges::any_of(ast.children, [](const auto & child) { return callsNow(*child); });
}

/// The columns that `ast` reads by qualified identifiers, by qualifier.
void collectColumns(const IAST & ast, std::map<String, std::set<String>> & columns)
{
    if (const auto * identifier = ast.as<ASTIdentifier>(); identifier && identifier->compound())
        columns[identifier->name_parts[0]].insert(identifier->name_parts[1]);
    for (const auto & child : ast.children)
        collectColumns(*child, columns);
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

ASTPtr parseSelect(const String & sql);

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

/// The table of `input` with the lightweight updates below `bounds`, `deltaQuerySnapshot('database', 'table', map(...))`,
/// or the table itself if it can't have updates.
ASTPtr makeSnapshot(const Input & input, const std::map<String, Int64> & bounds)
{
    const auto table_id = input.storage->getStorageID();
    if (!input.updatable)
        return make_intrusive<ASTTableIdentifier>(table_id);
    return makeASTFunction("deltaQuerySnapshot", make_intrusive<ASTLiteral>(Field(table_id.database_name)),
        make_intrusive<ASTLiteral>(Field(table_id.table_name)), makeBoundsMap(bounds));
}

/// `(alias._block_number, alias._block_offset) IN (SELECT _block_number, _block_offset FROM deltaQueryPatches(...))`, which
/// keeps the rows of `input` that its lightweight updates with versions in [`lower`, `upper`) change.
ASTPtr makeUpdatedRowsIn(const Input & input, const std::map<String, Int64> & lower, const std::map<String, Int64> & upper)
{
    const auto table_id = input.storage->getStorageID();
    auto patches = makeASTFunction("deltaQueryPatches", make_intrusive<ASTLiteral>(Field(table_id.database_name)),
        make_intrusive<ASTLiteral>(Field(table_id.table_name)), makeBoundsMap(lower), makeBoundsMap(upper));
    auto subquery = make_intrusive<ASTSubquery>(
        parseSelect(fmt::format("SELECT _block_number, _block_offset FROM {}", patches->formatWithSecretsOneLine())));
    return makeASTFunction("in",
        makeASTFunction("tuple", makeColumn(input.alias, "_block_number"), makeColumn(input.alias, "_block_offset")), std::move(subquery));
}

/// `(SELECT columns FROM table AS alias WHERE where UNION ALL ...)` over the pairs `sources` of a table of `input` and a
/// filter. The alias of the input lets the filters qualify its columns like the conjuncts of the query.
ASTPtr makeSemiJoinSubquery(const Input & input, const Names & columns, const std::vector<std::pair<ASTPtr, ASTPtr>> & sources)
{
    Strings names;
    for (const auto & column : columns)
        names.push_back(backQuote(column));
    Strings selects;
    for (const auto & [table, where] : sources)
        selects.push_back(fmt::format("SELECT {} FROM {} AS {} WHERE {}", fmt::join(names, ", "), table->formatWithSecretsOneLine(),
            backQuote(input.alias), where->formatWithSecretsOneLine()));
    return make_intrusive<ASTSubquery>(parseSelect(fmt::format("{}", fmt::join(selects, " UNION ALL "))));
}

/// `(k_1, ...) IN subquery` over the join keys of `input`.
ASTPtr makeJoinKeysIn(const Input & input, ASTPtr subquery)
{
    ASTs keys;
    for (const auto & key : input.join_keys)
        keys.push_back(makeColumn(input.alias, key));
    auto left = keys.size() == 1 ? std::move(keys[0]) : makeASTFunction("tuple", std::move(keys));
    return makeASTFunction("in", std::move(left), std::move(subquery));
}

/// Makes the rows of `table` that pass `conjuncts` the rows of `input` in `select`, whose WHERE gets `where`. The conjuncts
/// go to `where`, except for the right side of a LEFT, SEMI, ANTI or ASOF join, which needs its bounds before the join. In
/// WHERE they would filter the joined rows, and a left row whose matches are all out of bounds would get no row instead of
/// a row without a match. So the table of the right side becomes the subquery of those rows. Over a signed table the
/// subquery sums the signs of equal rows, because the join must see which rows remain, not each insert and retraction.
void setInput(ASTSelectQuery & select, const Input & input, ASTPtr table, ASTs conjuncts, ASTs & where, const String & sign_column)
{
    if (input.join_keys.empty())
    {
        table->setAlias(input.alias);
        setTable(findAliasedTable(select, input.alias), std::move(table));
        std::ranges::move(conjuncts, std::back_inserter(where));
        return;
    }
    Strings columns;
    Strings group_by;
    for (const auto & column : input.columns)
    {
        columns.push_back(fmt::format("{}.{} AS {}", backQuote(input.alias), backQuote(column), backQuote(column)));
        group_by.push_back(fmt::format("{}.{}", backQuote(input.alias), backQuote(column)));
    }
    const auto table_sql = table->formatWithSecretsOneLine();
    auto query = parseSelect(input.is_signed
        ? fmt::format("SELECT {0}, sum({1}.{2}) AS {2} FROM {3} AS {1} GROUP BY {4} HAVING {2} != 0",
            fmt::join(columns, ", "), backQuote(input.alias), backQuote(sign_column), table_sql, fmt::join(group_by, ", "))
        : fmt::format("SELECT {} FROM {} AS {}", fmt::join(columns, ", "), table_sql, backQuote(input.alias)));
    query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>().setExpression(
        ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(conjuncts)));
    auto subquery = make_intrusive<ASTSubquery>(std::move(query));
    subquery->setAlias(input.alias);
    setTable(findAliasedTable(select, input.alias), std::move(subquery));
}

/// The rows of `weighted`, which has `columns` and the weight `__weight` of each row, as rows of sign 1 or -1 in column
/// `sign`. Equal rows sum their weights first, so rows of opposite weights cancel.
String consolidate(const String & columns, const String & sign, const String & weighted)
{
    return fmt::format(
        "SELECT {0}, arrayJoin(arrayWithConstant(toUInt64(abs(`__count`)), toInt8(sign(`__count`)))) AS {1} "
        "FROM (SELECT {0}, sum(`__weight`) AS `__count` FROM ({2}) GROUP BY {0} HAVING `__count` != 0)",
        columns, sign, weighted);
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

String makeKeyName(size_t key) { return fmt::format("__key_{}", key + 1); }


/// Whether the core, the residual or the delta of the residual names its own columns `name`.
bool isGeneratedName(std::string_view name)
{
    for (std::string_view prefix : {"__key_", "__val_", "__agg_"})
        if (name.starts_with(prefix))
            return true;
    return name == "__count" || name == "__index" || name == "__weight";
}

/// A user name equal to a generated name would capture or shadow the generated column.
void checkNoGeneratedNames(const IAST & ast)
{
    auto check = [](std::string_view name)
    {
        if (isGeneratedName(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Name {} is reserved in queries with HAVING, ORDER BY, LIMIT, DISTINCT, windows or expressions over aggregates", name);
    };
    check(ast.tryGetAlias());
    if (const auto * identifier = ast.as<ASTIdentifier>())
        check(identifier->shortName());
    for (const auto & child : ast.children)
        checkNoGeneratedNames(*child);
}

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

/// Whether the rows that `select` drops stay dropped after inserts of more rows. LIMIT and LIMIT BY keep the first rows in
/// order, DISTINCT keeps one row per value, and a filter keeps the rows that pass it. A rank-like window over a partition
/// only grows for its rows as rows arrive, so QUALIFY with an upper bound on it keeps a prefix of each partition. Windows
/// combined with a clause that drops rows could read rows that the clause dropped, and so could OFFSET.
bool dropsRowsForGood(ASTSelectQuery & select, bool with_fill)
{
    if (select.limitOffset() || select.limitByOffset() || select.limitAfter() || select.limitUntil() || select.interpolate() || with_fill)
        return false;
    static const std::set<String> ranks = {"row_number", "rank", "dense_rank"};
    bool has_windows = false;
    bool only_ranks = true;
    visitResidualClauses(select, [&](const ASTPtr & node) -> ASTPtr
    {
        if (const auto * function = node->as<ASTFunction>(); function && function->isWindowFunction())
        {
            has_windows = true;
            only_ranks &= ranks.contains(function->name);
        }
        return nullptr;
    });
    if (!has_windows)
        return true;
    if (!only_ranks || select.limitLength() || select.limitBy() || select.distinct)
        return false;
    if (!select.qualify())
        return true;
    /// `rank <= c`, `rank < c`, `c >= rank` or `c > rank`.
    const auto * comparison = select.qualify()->as<ASTFunction>();
    if (!comparison || !comparison->arguments || comparison->arguments->children.size() != 2)
        return false;
    const auto & arguments = comparison->arguments->children;
    auto is_rank = [](const ASTPtr & ast)
    {
        const auto * function = ast->as<ASTFunction>();
        return function && function->isWindowFunction();
    };
    return ((comparison->name == "lessOrEquals" || comparison->name == "less") && is_rank(arguments[0]) && arguments[1]->as<ASTLiteral>())
        || ((comparison->name == "greaterOrEquals" || comparison->name == "greater") && is_rank(arguments[1]) && arguments[0]->as<ASTLiteral>());
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

    checkNoGeneratedNames(select);
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
    for (const auto & key : keys)
    {
        const auto & projection = outer.select()->children;
        auto hash = key->getTreeHash(true);
        auto column = std::ranges::find_if(projection, [&](const auto & expression) { return expression->getTreeHash(true) == hash; });
        residual.key_outputs.push_back(column == projection.end() ? "" : residual.outputs.at(column - projection.begin()));
    }
    residual.append_only = !is_signed && !residual.has_aggregates && dropsRowsForGood(outer, with_fill)
        && std::ranges::none_of(residual.key_outputs, [](const String & output) { return output.empty(); });

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
        const String suffix = suffixes.empty() ? "" : "If";
        auto name_of = [](const ASTPtr & column) { return column->as<ASTIdentifier &>().name(); };
        if (base == "count" && is_sum)
            return makeASTFunction("sum", add_core_aggregate(function.clone(), "sum"));
        if (base == "sum" && is_sum)
        {
            auto sum = add_core_aggregate(function.clone(), "sum");
            residual.sum_merges.push_back({residual.merges.size(), name_of(sum), "", makeASTFunction("count" + suffix, cloneAll(arguments))});
            return makeASTFunction("sum", std::move(sum));
        }
        if (base == "avg" && is_sum)
        {
            /// `avg(x)` is `sum(x) / count(x)`, which sums over rows.
            auto sum = add_core_aggregate(makeASTFunction("sum" + suffix, cloneAll(arguments)), "sum");
            auto count = add_core_aggregate(makeASTFunction("count" + suffix, cloneAll(arguments)), "sum");
            residual.sum_merges.push_back({residual.merges.size(), name_of(sum), name_of(count), nullptr});
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

    /// LIMIT and LIMIT BY keep any of the rows that tie in the ORDER BY. Ordering ties by the keys makes each refresh keep
    /// the same rows, so the target doesn't swap one tied row for another.
    if ((outer.limitLength() || outer.limitBy()) && !outer.limit_with_ties)
    {
        if (!outer.orderBy())
            outer.setExpression(ASTSelectQuery::Expression::ORDER_BY, make_intrusive<ASTExpressionList>());
        auto & elements = outer.orderBy()->children;
        std::set<String> ordered;
        for (const auto & element : elements)
            if (const auto * identifier = element->children.at(0)->as<ASTIdentifier>())
                ordered.insert(identifier->name());
        for (size_t i = 0; i < keys.size(); ++i)
        {
            if (ordered.contains(makeKeyName(i)))
                continue;
            auto element = make_intrusive<ASTOrderByElement>();
            element->direction = 1;
            element->nulls_direction = 1;
            element->children.push_back(make_intrusive<ASTIdentifier>(makeKeyName(i)));
            elements.push_back(std::move(element));
        }
    }

    auto having = outer.having();
    outer.setExpression(ASTSelectQuery::Expression::HAVING, nullptr);
    outer.setExpression(ASTSelectQuery::Expression::WHERE, std::move(having));
    for (auto clause : {ASTSelectQuery::Expression::WITH, ASTSelectQuery::Expression::PREWHERE, ASTSelectQuery::Expression::GROUP_BY,
            ASTSelectQuery::Expression::SETTINGS, ASTSelectQuery::Expression::TABLES})
        outer.setExpression(clause, nullptr);
    outer.group_by_all = false;
    for (size_t i = 0; i < residual.outputs.size(); ++i)
        outer.select()->children[i]->setAlias(residual.outputs[i]);
    residual.outer = std::move(outer_ast);

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

/// `SELECT columns FROM cluster(cluster, view(query))`. It runs `query` on one replica of each shard, and all shards must
/// run it. This server runs its own shard over a connection too. Then `shardNum()` in `query` is the number of the shard.
/// A local read would reuse the table functions of `query` that this server evaluates for the header, with `shardNum()` 0.
String readShards(const String & cluster, const String & columns, const String & query)
{
    return fmt::format("SELECT {} FROM cluster({}, view({})) SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 0",
        columns, quoteString(cluster), query);
}

/// The expression that each shard evaluates to its cursor in `cursor`, the subtree of its number.
String makeShardCursor(const CursorTreeNodePtr & cursor)
{
    Strings entries;
    for (const auto & entry : cursor ? cursorTreeToMap(cursor) : Map{})
    {
        const auto & pair = entry.safeGet<Tuple>();
        entries.push_back(fmt::format("{}, {}", quoteString(pair.at(0).safeGet<String>()), pair.at(1).safeGet<Int64>()));
    }
    return fmt::format("mapApply((key, value) -> (substring(key, length(toString(shardNum())) + 2), value), "
        "mapFilter((key, value) -> startsWith(key, concat(toString(shardNum()), '.')), CAST(map({}), 'Map(String, Int64)')))",
        fmt::join(entries, ", "));
}

}

void checkReadableAsOf(const MergeTreeData & storage, const DataPartsVector & parts, const std::map<String, Int64> & bounds)
{
    for (const auto & part : parts)
    {
        if (part->info.isPatch() && storage.supportsReplication())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support replicated table {}, which has lightweight updates",
                storage.getStorageID().getNameForLogs());
        const auto range = getBlockRange(*part);
        /// The last update or mutation that the part holds.
        const Int64 last = part->info.isPatch() ? range.max_block : part->info.mutation;
        const Int64 bound = getBound(bounds, range.getPartitionId());
        if (range.min_block < bound && bound <= last)
            throw Exception(ErrorCodes::DELTA_QUERY_HISTORY_LOST,
                "Part {} of table {} no longer holds the rows as of block {} of partition {}, because a merge or a mutation wrote "
                "later changes into it", part->name, storage.getStorageID().getNameForLogs(), bound, range.getPartitionId());
    }
}

DataPartsVector getPatchParts(const MergeTreeData & storage, const std::map<String, Int64> & lower, const std::map<String, Int64> & upper)
{
    auto patches = storage.getPatchPartsVectorForInternalUsage();
    checkReadableAsOf(storage, patches, lower);
    checkReadableAsOf(storage, patches, upper);
    std::erase_if(patches, [&](const auto & patch)
    {
        const auto range = getBlockRange(*patch);
        return range.min_block < getBound(lower, range.getPartitionId()) || range.max_block >= getBound(upper, range.getPartitionId());
    });
    return patches;
}

DeltaQuery::DeltaQuery(const ASTPtr & query_, const ContextPtr & context_)
{
    /// The query can set `delta_query_sign_column` in its SETTINGS, as the query of a view does.
    auto context = Context::createCopy(context_);
    InterpreterSetQuery::applySettingsFromQuery(query_, context);
    if (auto cte = getRecursiveCTE(*query_))
    {
        auto split = std::make_shared<Recursion>();
        const auto & parts = cte->parts;
        auto check_part = [](const DeltaQuery & part)
        {
            if (part.residual || part.recursion)
                throwUnsupported("recursive CTEs with HAVING, ORDER BY, LIMIT, DISTINCT, windows or WITH RECURSIVE");
            if (!part.cluster.empty())
                throwUnsupported("recursive CTEs over Distributed tables");
        };
        split->base = std::make_unique<DeltaQuery>(makeSelectWithUnion(parts[0]), context);
        check_part(*split->base);

        const String & table_name = context->getSettingsRef()[Setting::delta_query_recursion_table];
        ASTPtr table;
        if (!table_name.empty())
        {
            ParserCompoundIdentifier parser(/*table_name_with_optional_uuid=*/true);
            table = parseQuery(parser, table_name, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            const auto table_id = context->resolveStorageID(table->as<ASTTableIdentifier &>().getTableId());
            split->key = toString(DatabaseCatalog::instance().getTable(table_id, context)->getStorageID().uuid);
        }
        for (size_t i = 1; i < parts.size() && table; ++i)
        {
            auto step = parts[i]->clone();
            auto & expression = *findTable(step->as<ASTSelectQuery &>(), isTableNamed(cte->name));
            auto alias = expression.database_and_table_name->tryGetAlias();
            auto replacement = table->clone();
            replacement->setAlias(alias.empty() ? cte->name : alias);
            setTable(expression, std::move(replacement));

            auto & part = *split->steps.emplace_back(std::make_unique<DeltaQuery>(makeSelectWithUnion(std::move(step)), context));
            check_part(part);
            auto reads_table = [&](const Input & input) { return input.key == split->key; };
            auto input = std::ranges::find_if(part.inputs, reads_table);
            if (std::ranges::count_if(part.inputs, reads_table) != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "A step of the recursive CTE reads table {} of delta_query_recursion_table", table_name);
            if (!input->join_keys.empty())
                throwUnsupported("recursive CTEs whose step reads the CTE on the right of a LEFT, SEMI, ANTI or ASOF join");
            if (!part.sign_column.empty() && !input->is_signed)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} of delta_query_recursion_table needs the column {} of delta_query_sign_column",
                    table_name, part.sign_column);
            input->lookup = true;
        }
        recursion = std::move(split);
        return;
    }
    checkNoSubqueries(*query_);
    const String sign_column_setting = context->getSettingsRef()[Setting::delta_query_sign_column];
    shard_query = readLocalTables(query_, context, cluster);
    shard_context = context;

    Residual split;
    split.sign_column = sign_column_setting;
    auto core = analyze(shard_query ? shard_query : query_, context, sign_column_setting, sign_column_setting.empty() ? nullptr : &split);
    if (!core)
        return;
    if (shard_query)
        shard_query = core;
    analyze(core, context, sign_column_setting, nullptr);
    /// The deltas of the core bind the sign of each term. The sign doesn't change the types.
    auto get_core_columns = [&]
    {
        auto term = query->clone();
        bindTerm(*term, Int64{1}, {});
        return InterpreterSelectQueryAnalyzer::getSampleBlock(term, context)->getNamesAndTypesList();
    };
    auto core_columns = get_core_columns();

    /// A sum of Nullable values is NULL over rows whose values are all NULL, but the merged sum of the rows and their
    /// retractions is 0. So the merge returns NULL where the count of non-NULL values is 0.
    bool added_counts = false;
    for (auto & sum_merge : split.sum_merges)
    {
        if (!removeLowCardinality(core_columns.tryGetByName(sum_merge.sum_column)->type)->isNullable())
            continue;
        if (sum_merge.count)
        {
            sum_merge.count_column = fmt::format("__agg_{}", split.simple_functions.size());
            sum_merge.count->setAlias(sum_merge.count_column);
            core->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>().select()->children.push_back(sum_merge.count);
            split.simple_functions.push_back("sum");
            added_counts = true;
        }
        auto & merge = split.merges.at(sum_merge.merge);
        merge = makeASTFunction("if", makeASTFunction("equals", makeASTFunction("sum", make_intrusive<ASTIdentifier>(sum_merge.count_column)),
            make_intrusive<ASTLiteral>(UInt64{0})), make_intrusive<ASTLiteral>(Field()), merge);
    }
    if (added_counts)
    {
        analyze(core, context, sign_column_setting, nullptr);
        core_columns = get_core_columns();
    }

    /// The integral has the columns of the deltas of the core, and merges the aggregate columns.
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
    split.output_query = parseSelect(fmt::format("SELECT *, toInt8(1) AS {} FROM ({})", backQuote(sign_column_setting),
        split.buildResidual(split.buildInput(fmt::format("null({})", quoteString(joinNames(structure))), ""))));
    for (const auto & column : *InterpreterSelectQueryAnalyzer::getSampleBlock(split.output_query, context))
        if (WhichDataType(column.type).isAggregateFunction())
            throwUnsupported(fmt::format("aggregate states like column {} in the results of queries with HAVING, ORDER BY, LIMIT, DISTINCT, windows or expressions over aggregates", column.name));
    residual = std::make_shared<const Residual>(std::move(split));
}

ASTPtr DeltaQuery::analyze(const ASTPtr & query_, const ContextPtr & context, const String & sign_column_, Residual * split)
{
    inputs.clear();
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
    }
    /// The delta of a join that isn't linear in a table, of a filter by now(), or of lightweight updates retracts rows of the
    /// result. A query with `delta_query_sign_column` reads the updates of the tables that can have them.
    const bool retracts = has_clock || std::ranges::any_of(inputs, [&](const auto & input)
    {
        return !input.lookup && (!input.join_keys.empty() || (input.updatable && !sign_column.empty()));
    });
    if (retracts && sign_column.empty())
        throwUnsupported("LEFT, SEMI, ANTI or ASOF joins of tables that aren't lookup tables, or now(), without delta_query_sign_column");

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
    const bool has_residual = split && needsResidual(*query_node, !signs.empty());
    checkExpressions(*query_tree, input_indexes, is_linear && !has_residual);
    if (has_residual)
    {
        auto resolved = query_tree->toAST(ast_options);
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
    /// A reduction of the right table of a LEFT, SEMI, ANTI or ASOF join by an equality outside its ON could remove the match
    /// of a left row, which would then join with default values instead.
    std::erase_if(semi_join_edges, [&](const auto & edge)
    {
        return !inputs[edge.to].join_keys.empty() && !std::ranges::contains(keyed_edges, edge);
    });
    query = query_tree->toAST(ast_options);

    auto & select = query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
    /// Without GROUP BY, a delta that reads no rows still returns one row by default, with 0 for `min` and `max`.
    if (is_linear && !select.groupBy())
        setSetting(select, "empty_result_for_aggregation_by_empty_set", true);
    if (!sign_column.empty())
        addWeights(select, *query_node, signs, sign_column);

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
    return nullptr;
}

DeltaQuery::~DeltaQuery() = default;

ASTPtr DeltaQuery::getQuery() const
{
    if (recursion)
        return recursion->base->getQuery();
    /// The weights of the query read `__term_sign`, which each term of a delta binds. The query itself is one term of sign 1.
    auto result = (residual ? residual->output_query : query)->clone();
    bindTerm(*result, 1, {});
    return result;
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

String DeltaQuery::Residual::buildAppendOnlyInput(const String & target, const String & where, const String & delta) const
{
    Strings outputs_list;
    for (const auto & output : outputs)
        outputs_list.push_back(backQuote(output));
    Strings state;
    Strings keys;
    for (size_t i = 0; i < num_keys; ++i)
    {
        state.push_back(fmt::format("{} AS {}", backQuote(key_outputs.at(i)), backQuote(makeKeyName(i))));
        keys.push_back(backQuote(makeKeyName(i)));
    }
    /// A row of the target with signs that sum to n stands for n rows, and so does a row of the delta with count n.
    return fmt::format(
        "SELECT {0} FROM (SELECT {1}, sum({2}) AS `__count` FROM {3}{4} GROUP BY {1} HAVING `__count` > 0) "
        "ARRAY JOIN range(toUInt64(`__count`)) AS `__index` "
        "UNION ALL SELECT {5} FROM {6} ARRAY JOIN range(toUInt64(`__agg_0`)) AS `__index`",
        joinNames(state), joinNames(outputs_list), backQuote(sign_column), target, where, joinNames(keys), delta);
}

String DeltaQuery::Residual::buildResidual(const String & input) const
{
    auto parsed = parseSelect(fmt::format("SELECT * FROM ({})", input));
    auto result = outer->clone();
    result->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::TABLES,
        parsed->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>().tables()->clone());
    return result->formatWithSecretsOneLine();
}

bool DeltaQuery::isAppendOnly() const
{
    return residual && residual->append_only;
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
    return fmt::format("CREATE TABLE IF NOT EXISTS {} UUID {} ({}) ENGINE = AggregatingMergeTree ORDER BY {} SETTINGS allow_nullable_key = 1",
        integral.getFullTableName(), quoteString(toString(integral.uuid)), joinNames(columns), order.empty() ? "tuple()" : "(" + joinNames(order) + ")");
}

/// The residual over the touched keys counts each of its rows once, and the target counts each of its rows of those keys by
/// the sum of their signs. The difference of the counts per row is the delta, which becomes that many rows of sign 1 or -1.
ASTPtr DeltaQuery::buildResidualDelta(const StorageID & integral, const StorageID & delta, bool delta_is_empty, const StorageID & target,
    bool all_keys) const
{
    chassert(residual);
    String where;
    String target_where;
    if (!all_keys && !residual->partition_keys.empty())
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
    /// An empty delta leaves the residual unchanged, but a global aggregate has a row before its first delta.
    const bool skip = delta_is_empty && !(residual->has_aggregates && residual->num_keys == 0);
    const auto input = residual->append_only
        ? residual->buildAppendOnlyInput(target.getFullTableName(), target_where, delta.getFullTableName())
        : residual->buildInput(integral.getFullTableName(), where);
    return parseSelect(consolidate(columns, sign, fmt::format(
        "SELECT {0}, toInt64(1) AS `__weight` FROM ({2}) UNION ALL SELECT {0}, -toInt64({1}) AS `__weight` FROM {3}{4}",
        columns, sign, residual->buildResidual(input), target.getFullTableName(), target_where)) + (skip ? " WHERE 0" : ""));
}

bool DeltaQuery::canCorrect() const
{
    if (recursion)
        return false;
    if (residual)
        return !residual->append_only;
    return !consolidated_columns.empty() || !summed_columns.empty();
}

ASTPtr DeltaQuery::buildCorrection(const CursorTreeNodePtr & new_cursor, const StorageID & target) const
{
    chassert(canCorrect() && !residual);
    const auto query_rows = buildDelta(nullptr, new_cursor)->formatWithSecretsOneLine();
    const auto target_name = target.getFullTableName();
    if (!consolidated_columns.empty())
    {
        Strings columns;
        for (const auto & column : consolidated_columns)
            columns.push_back(backQuote(column));
        const auto sign = backQuote(sign_column);
        return parseSelect(consolidate(joinNames(columns), sign, fmt::format(
            "SELECT {0}, toInt64({1}) AS `__weight` FROM ({2}) UNION ALL SELECT {0}, -toInt64({1}) AS `__weight` FROM {3}",
            joinNames(columns), sign, query_rows, target_name)));
    }
    /// Each group gets one row with the sums of the query minus the sums of the target. A target column can have an unsigned
    /// type that holds negative sums modulo its range. The products with a signed factor have one type in both selects.
    Strings columns;
    Strings retracted;
    Strings results;
    Strings keys;
    Strings nonzero;
    for (const auto & [name, is_sum] : summed_columns)
    {
        const auto column = backQuote(name);
        if (is_sum)
        {
            columns.push_back(fmt::format("{0} * toInt8(1) AS {0}", column));
            retracted.push_back(fmt::format("{} * toInt8(-1)", column));
            results.push_back(fmt::format("sum({0}) AS {0}", column));
            nonzero.push_back(fmt::format("{} != 0", column));
        }
        else
        {
            columns.push_back(column);
            retracted.push_back(column);
            results.push_back(column);
            keys.push_back(column);
        }
    }
    return parseSelect(fmt::format("SELECT {} FROM (SELECT {} FROM ({}) UNION ALL SELECT {} FROM {}){} HAVING {}",
        joinNames(results), joinNames(columns), query_rows, joinNames(retracted), target_name,
        keys.empty() ? "" : " GROUP BY " + joinNames(keys), fmt::join(nonzero, " OR ")));
}

bool DeltaQuery::hasPartitionedResidual() const
{
    return residual && !residual->partition_keys.empty();
}

CursorTreeNodePtr DeltaQuery::restrictCursor(const CursorTreeNodePtr & tree) const
{
    if (!tree)
        return nullptr;
    auto result = std::make_shared<CursorTreeNode>();
    for (const auto & input : inputs)
        if (tree->hasSubtree(input.key))
            result->setSubtree(input.key, tree->getSubtree(input.key));
    if (tree->hasValue(time_key))
        result->setValue(time_key, tree->getValue(time_key));
    return result;
}

CursorTreeNodePtr DeltaQuery::getFrontier(const CursorTreeNodePtr & from) const
{
    /// The cursor has the inputs of the base and of the steps, but not the table that holds `r`.
    if (recursion)
    {
        recursion->checkTable();
        auto tree = recursion->base->getFrontier(recursion->base->restrictCursor(from));
        for (const auto & step : recursion->steps)
            for (const auto & [key, value] : *step->getFrontier(step->restrictCursor(from)))
            {
                if (key == recursion->key || tree->hasSubtree(key) || tree->hasValue(key))
                    continue;
                if (const auto * node = std::get_if<CursorTreeNodePtr>(&value))
                    tree->setSubtree(key, *node);
                else
                    tree->setValue(key, std::get<Int64>(value));
            }
        return tree;
    }
    if (!cluster.empty())
    {
        auto tree = from ? from->clone() : std::make_shared<CursorTreeNode>();
        const auto shard_frontiers = readShards(cluster, "_shard_num, cursor", fmt::format("SELECT cursor FROM deltaQueryFrontier(({}), {})",
            shard_query->formatWithSecretsOneLine(), makeShardCursor(from)));
        InterpreterSelectQueryAnalyzer interpreter(parseSelect(shard_frontiers), shard_context, SelectQueryOptions());
        auto io = interpreter.execute();
        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
            for (size_t row = 0; row < block.rows(); ++row)
                tree->setSubtree(toString((*block.getByPosition(0).column)[row].safeGet<UInt64>()),
                    buildCursorTree((*block.getByPosition(1).column)[row].safeGet<Map>()));
        return tree;
    }
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
    auto tree = makeCursorTree(frontier);
    if (clock_input)
        tree->setValue(time_key, std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count());
    return tree;
}

std::vector<StreamSubscriptionPtr> DeltaQuery::subscribeToCommits(const CursorTreeNodePtr & cursor, std::function<void()> callback) const
{
    if (recursion)
    {
        recursion->checkTable();
        auto subscriptions = recursion->base->subscribeToCommits(recursion->base->restrictCursor(cursor), callback);
        for (const auto & step : recursion->steps)
            std::ranges::move(step->subscribeToCommits(step->restrictCursor(cursor), callback), std::back_inserter(subscriptions));
        return subscriptions;
    }
    if (!cluster.empty())
        return {};
    const auto tables = snapshotTables(inputs);
    const auto bounds = parseCursor(cursor, inputs, tables, "cursor");
    std::vector<StreamSubscriptionPtr> subscriptions;
    std::set<String> keys;
    for (const auto & input : inputs)
    {
        if (input.lookup || !keys.insert(input.key).second)
            continue;
        /// The subscription starts at the last block of each partition that the cursor includes, so it runs the callback only
        /// for blocks after the cursor.
        auto subscription = std::make_shared<MergeTreeBoundsSubscription>(1, 0);
        std::map<std::string, int64_t> last_blocks;
        for (const auto & [partition_id, bound] : bounds.at(input.key))
            last_blocks[partition_id] = bound - 1;
        subscription->update(std::move(last_blocks), {});
        subscription->setCallback(callback);
        input.storage->subscription_manager.registerSubscription(subscription);
        input.storage->triggerStreamingSubscriptionEnrichment();
        subscriptions.push_back(std::move(subscription));
    }
    return subscriptions;
}

std::vector<scope_guard> DeltaQuery::holdCursor(const CursorTreeNodePtr & cursor) const
{
    if (recursion)
    {
        auto guards = recursion->base->holdCursor(recursion->base->restrictCursor(cursor));
        for (const auto & step : recursion->steps)
            std::ranges::move(step->holdCursor(step->restrictCursor(cursor)), std::back_inserter(guards));
        return guards;
    }
    if (!cluster.empty())
        return {};
    const auto tables = snapshotTables(inputs);
    const auto bounds = parseCursor(cursor, inputs, tables, "cursor");
    std::vector<scope_guard> guards;
    std::set<String> keys;
    for (const auto & input : inputs)
        if (auto storage = std::dynamic_pointer_cast<StorageMergeTree>(input.storage); storage && !input.lookup && keys.insert(input.key).second)
            guards.push_back(storage->holdDeltaQueryBounds(bounds.at(input.key)));
    return guards;
}

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
///
/// A LEFT, SEMI, ANTI or ASOF join is linear in its left side only. A change of its right input i changes the result for
/// every left row of its join keys. So term i is the difference of two terms that read input i as of the new and as of the
/// old cursor, restricted to the join keys of the changes of input i. The reduction starts from those changes.
///
/// A filter by now() is a join with the time, which is one more input after the others. Its term reads every input below
/// the old cursor, and it is the difference of the query at the new and at the old time over the rows whose filter changed.
ASTPtr DeltaQuery::buildDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    if (recursion)
        return buildRecursiveDelta(old_cursor, new_cursor);
    if (!cluster.empty())
        return parseSelect(readShards(cluster, "*", fmt::format("SELECT * FROM deltaQuery(({}), {}, {})",
            shard_query->formatWithSecretsOneLine(), makeShardCursor(old_cursor), makeShardCursor(new_cursor))));
    const auto tables = snapshotTables(inputs);
    const auto old_bounds = parseCursor(old_cursor, inputs, tables, "old cursor");
    const auto new_bounds = parseCursor(new_cursor, inputs, tables, "new cursor");

    /// Term i reads the blocks of input i in [old, new). In a join, the other terms read the blocks below old or new, and the
    /// updates of an input read its rows below old.
    const bool is_join = inputs.size() > 1;
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

    const auto & select = query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
    auto terms = make_intrusive<ASTExpressionList>();
    bool has_pairs = false;

    /// The rows of an input that a term reads: the rows below old as of old, below new as of new, in [old, new) as of new, and
    /// below old as of new.
    enum class Rows : uint8_t { Old, New, Delta, Prior };
    /// The rows of an input that also pass `filters`.
    struct Selection
    {
        Rows rows;
        ASTs filters;
    };
    auto add_rows = [&](ASTs & conjuncts, size_t j, Rows rows)
    {
        const auto & key = inputs[j].key;
        addBounds(conjuncts, inputs[j].alias, rows == Rows::Delta ? &old_bounds.at(key) : nullptr,
            rows == Rows::Old || rows == Rows::Prior ? old_bounds.at(key) : new_bounds.at(key));
    };
    /// The table of input j as of the cursor of `rows`.
    auto table_at = [&](size_t j, Rows rows)
    {
        const auto & key = inputs[j].key;
        return makeSnapshot(inputs[j], rows == Rows::Old ? old_bounds.at(key) : new_bounds.at(key));
    };
    /// The table and the filter of each selection of input j.
    auto get_sources = [&](size_t j, const std::vector<Selection> & selections)
    {
        std::vector<std::pair<ASTPtr, ASTPtr>> sources;
        for (const auto & [rows, filters] : selections)
        {
            ASTs conjuncts;
            add_rows(conjuncts, j, rows);
            for (const auto & filter : filters)
                conjuncts.push_back(filter->clone());
            sources.emplace_back(table_at(j, rows), makeConjunction(std::move(conjuncts)));
        }
        return sources;
    };

    /// Adds the term that reads rows `rows[j]` of each input j, and of input `from` only those that also pass `own`. The
    /// reduction starts from the selections `change` of input `from`, or without them from its rows in the term. The rows of
    /// the term have weight `sign` times their weight in the query, and now() is `time`.
    auto add_term = [&](const std::vector<Rows> & rows, std::optional<size_t> from, ASTs own, const std::vector<Selection> & change,
        Int64 sign, std::optional<Int64> time)
    {
        std::vector<ASTs> conjuncts(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            add_rows(conjuncts[j], j, rows[j]);
        if (from)
            std::ranges::move(own, std::back_inserter(conjuncts[*from]));

        std::vector<bool> reached(inputs.size());
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
            for (const auto & [edge_from, x, b, y] : semi_join_edges)
            {
                if (edge_from != a || reached[b])
                    continue;
                reached[b] = true;
                queue.push_back(b);
                auto sources = a == *from && !change.empty() ? get_sources(a, change)
                    : std::vector<std::pair<ASTPtr, ASTPtr>>{{table_at(a, rows[a]), makeConjunction(cloneAll(conjuncts[a]))}};
                conjuncts[b].push_back(makeASTFunction("in", makeColumn(inputs[b].alias, y), makeSemiJoinSubquery(inputs[a], {x}, sources)));
            }
        }

        auto term = select.clone();
        auto & term_select = term->as<ASTSelectQuery &>();
        ASTs where;
        if (select.where())
            where.push_back(select.where()->clone());
        for (size_t j = 0; j < inputs.size(); ++j)
            setInput(term_select, inputs[j], table_at(j, rows[j]), std::move(conjuncts[j]), where, sign_column);
        term_select.setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
        bindTerm(*term, sign, time);
        terms->children.push_back(std::move(term));
    };

    /// From the empty cursor, the delta is the query as of the new cursor. One term reads it, instead of one term per input
    /// that each read all the other inputs.
    if (from_empty)
        add_term(std::vector<Rows>(inputs.size(), Rows::New), {}, {}, {}, 1, new_time);

    for (size_t i = 0; i < inputs.size() && !from_empty; ++i)
    {
        /// The changes of a lookup table join with nothing, and the other terms read it as of the new cursor.
        if (inputs[i].lookup)
            continue;
        const auto & lower = old_bounds.at(inputs[i].key);
        const auto & upper = new_bounds.at(inputs[i].key);
        const auto & blocks = tables.at(inputs[i].key);
        const bool updated = blocks.hasBlocks(lower, upper, /*updates=*/ true);
        if (updated && sign_column.empty())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries read the lightweight updates of table {} only with delta_query_sign_column",
                inputs[i].storage->getStorageID().getNameForLogs());
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
            continue;
        std::vector<Rows> rows(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            rows[j] = j < i && !inputs[j].lookup ? Rows::Old : Rows::New;
        if (inputs[i].join_keys.empty())
        {
            for (const auto & [own_rows, filters] : changes)
            {
                rows[i] = own_rows;
                add_term(rows, i, cloneAll(filters), {}, own_rows == Rows::Old ? -1 : 1, new_time);
            }
            has_pairs |= updated;
            continue;
        }
        auto keys = makeJoinKeysIn(inputs[i], makeSemiJoinSubquery(inputs[i], inputs[i].join_keys, get_sources(i, changes)));
        for (auto [own_rows, sign] : {std::pair{Rows::New, Int64{1}}, std::pair{Rows::Old, Int64{-1}}})
        {
            rows[i] = own_rows;
            add_term(rows, i, {keys->clone()}, changes, sign, new_time);
        }
        has_pairs = true;
    }

    if (clock_input && !from_empty && *old_time != *new_time)
    {
        std::vector<Rows> rows(inputs.size());
        for (size_t j = 0; j < inputs.size(); ++j)
            rows[j] = inputs[j].lookup ? Rows::New : Rows::Old;
        auto filter_at = [&](Int64 time)
        {
            /// A conjunct with now() is a function of it, so `bindTerm` reaches every call.
            auto filter = makeConjunction(cloneAll(clock_conjuncts));
            bindTerm(*filter, {}, time);
            return filter;
        };
        auto old_filter = filter_at(*old_time);
        auto new_filter = filter_at(*new_time);
        auto changed = makeASTFunction("and", makeASTFunction("or", old_filter->clone(), new_filter->clone()),
            makeASTFunction("not", makeASTFunction("and", old_filter, new_filter)));
        for (auto [time, sign] : {std::pair{*new_time, Int64{1}}, std::pair{*old_time, Int64{-1}}})
            add_term(rows, clock_input, {changed->clone()}, {}, sign, time);
        has_pairs = true;
    }

    /// No input has new rows.
    if (terms->children.empty())
    {
        auto term = select.clone();
        term->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::WHERE, make_intrusive<ASTLiteral>(Field(UInt8{0})));
        bindTerm(*term, 1, new_time);
        terms->children.push_back(std::move(term));
    }

    auto result = make_intrusive<ASTSelectWithUnionQuery>();
    result->union_mode = SelectUnionMode::UNION_ALL;
    result->is_normalized = true;
    result->list_of_selects = terms;
    result->children.push_back(terms);
    if (!has_pairs || consolidated_columns.empty())
        return result;
    Strings columns;
    for (const auto & column : consolidated_columns)
        columns.push_back(backQuote(column));
    return parseSelect(consolidate(joinNames(columns), backQuote(sign_column), fmt::format("SELECT {}, toInt64({}) AS `__weight` FROM ({})",
        joinNames(columns), backQuote(sign_column), result->formatWithSecretsOneLine())));
}

/// `WITH RECURSIVE d AS (SELECT * FROM (d_0) UNION ALL step(d) ...) SELECT * FROM d`. The delta `d_0` is the delta of the
/// base plus the delta of each step. The steps read all rows of the table that holds `r` at the old cursor.
ASTPtr DeltaQuery::buildRecursiveDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    recursion->checkTable();
    constexpr auto cte = "__recursion";
    const auto & base = *recursion->base;
    Strings anchors{base.buildDelta(base.restrictCursor(old_cursor), base.restrictCursor(new_cursor))->formatWithSecretsOneLine()};
    Strings terms;
    for (const auto & step : recursion->steps)
    {
        auto step_cursor = new_cursor ? step->restrictCursor(new_cursor) : std::make_shared<CursorTreeNode>();
        if (auto frontier = step->getFrontier(nullptr); frontier->hasSubtree(recursion->key))
            step_cursor->setSubtree(recursion->key, frontier->getSubtree(recursion->key));
        anchors.push_back(step->buildDelta(step->restrictCursor(old_cursor), step_cursor)->formatWithSecretsOneLine());
        terms.push_back(step->buildRecursiveTerm(step_cursor, recursion->key, cte)->formatWithSecretsOneLine());
    }
    return parseSelect(fmt::format("WITH RECURSIVE {0} AS (SELECT * FROM ({1}) UNION ALL {2}) SELECT * FROM {0}",
        cte, fmt::join(anchors, " UNION ALL "), fmt::join(terms, " UNION ALL ")));
}

ASTPtr DeltaQuery::buildRecursiveTerm(const CursorTreeNodePtr & new_cursor, const String & key, const String & cte) const
{
    const auto tables = snapshotTables(inputs);
    const auto bounds = parseCursor(new_cursor, inputs, tables, "new cursor");
    const auto & select = query->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
    auto term = select.clone();
    auto & term_select = term->as<ASTSelectQuery &>();
    ASTs where;
    if (select.where())
        where.push_back(select.where()->clone());
    for (const auto & input : inputs)
    {
        if (input.key == key)
        {
            auto table = make_intrusive<ASTTableIdentifier>(cte);
            table->setAlias(input.alias);
            setTable(findAliasedTable(term_select, input.alias), std::move(table));
            continue;
        }
        ASTs conjuncts;
        addBounds(conjuncts, input.alias, nullptr, bounds.at(input.key));
        setInput(term_select, input, makeSnapshot(input, bounds.at(input.key)), std::move(conjuncts), where, sign_column);
    }
    term_select.setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
    bindTerm(*term, 1, getTime(new_cursor));
    return term;
}

}
