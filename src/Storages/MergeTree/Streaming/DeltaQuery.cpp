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
#include <Analyzer/createUniqueAliasesIfNecessary.h>
#include <Core/Joins.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/Streaming/Subscription/SubscriptionEnrichment.h>
#include <Storages/StorageProxy.h>

#include <deque>
#include <map>
#include <unordered_map>


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

namespace
{

using Input = DeltaQuery::Input;
using SemiJoinEdge = DeltaQuery::SemiJoinEdge;

/// Table UUID -> partition ID -> first excluded block number.
using Cursor = std::map<String, std::map<String, Int64>>;

/// The inputs by their nodes in the query tree.
using InputIndexes = std::unordered_map<const IQueryTreeNode *, size_t>;

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

/// Rejects tables outside FROM, like in `k IN t`, which the cursors do not cover. A join query also rejects window
/// functions, whose result over the union of the terms differs from the union of their results.
void checkExpressions(const IQueryTreeNode & node, const InputIndexes & input_indexes, bool is_join)
{
    for (const auto & child : node.getChildren())
    {
        if (!child)
            continue;
        auto type = child->getNodeType();
        if ((type == QueryTreeNodeType::TABLE || type == QueryTreeNodeType::TABLE_FUNCTION) && !input_indexes.contains(child.get()))
            throwUnsupported("IN over tables");
        if (const auto * function = child->as<FunctionNode>(); is_join && function && function->isWindowFunction())
            throwUnsupported("window functions in joins");
        checkExpressions(*child, input_indexes, is_join);
    }
}

/// Checks the clauses whose result over the union of the terms of a join query differs from the union of their results.
void checkJoinClauses(const QueryNode & query)
{
    if (query.isDistinct())
        throwUnsupported("DISTINCT in joins");
    if (query.isGroupByWithTotals() || query.isGroupByWithRollup() || query.isGroupByWithCube() || query.isGroupByWithGroupingSets())
        throwUnsupported("WITH TOTALS, ROLLUP, CUBE or GROUPING SETS in joins");
    if (query.hasHaving())
        throwUnsupported("HAVING in joins");
    if (query.hasOrderBy() || query.hasLimit() || query.hasOffset() || query.hasLimitByLimit() || query.hasLimitAfter()
        || query.hasLimitUntil())
        throwUnsupported("ORDER BY, LIMIT or LIMIT BY in joins");
    if (query.hasWindow() || query.hasQualify())
        throwUnsupported("WINDOW or QUALIFY in joins");
}

/// Checks that each result column of a join query is either a mergeable aggregate or free of aggregates. Resolution inlines
/// aliases, so `sum(v) AS s, s + 1` fails too. The query tree skips optimizations, which move arithmetic out of aggregates
/// and would make `sum(v * 2)` fail.
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
/// `-0.0 IN (SELECT 0.0)` is false. So only integer and string columns of one type count as join keys.
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
            || !(type->isValueRepresentedByInteger() || isStringOrFixedString(type)))
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
{
    checkNoSubqueries(*query_);
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

    const bool is_join = inputs.size() > 1;
    checkExpressions(*query_tree, input_indexes, is_join);
    if (is_join)
    {
        checkJoinClauses(*query_node);
        checkResultColumns(*query_node);
    }

    if (query_node->hasWhere())
        conditions.push_back(query_node->getWhere());
    semi_join_edges = collectSemiJoinEdges(conditions, input_indexes);
    query = query_tree->toAST();

    /// Resolution checks only that the user can see the tables. Building the plan checks the grants on the columns that
    /// the query reads.
    interpreter.getSampleBlock();
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
