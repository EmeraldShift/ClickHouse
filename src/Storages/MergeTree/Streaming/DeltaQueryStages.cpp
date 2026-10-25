#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Common/quoteString.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/TypeTree.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageProxy.h>


#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsString delta_query_recursion_table;
}

using namespace DeltaQueryInternal;

namespace
{

/// Calls `f` for each table expression in FROM of `select` that is a subquery, with its 1-based index among them.
void forEachSubqueryInFrom(const ASTSelectQuery & select, const std::function<void(ASTTableExpression &, size_t)> & f)
{
    if (!select.tables())
        return;
    size_t index = 0;
    for (const auto & element : select.tables()->children)
        if (const auto & table_expression = element->as<ASTTablesInSelectQueryElement &>().table_expression)
        {
            auto & expression = table_expression->as<ASTTableExpression &>();
            if (expression.table_function)
                throwUnsupported("table functions in FROM");
            if (expression.subquery)
                f(expression, ++index);
        }
}

/// The stage table of the stage of path `path` of `view`, see `DeltaQuery::Stage`. Its UUID follows from the UUID of the
/// view, like the UUID of the integral of the view.
StorageID makeStageTableId(const StorageID & view, const String & path)
{
    SipHash hash;
    hash.update(view.uuid);
    hash.update("stage");
    hash.update(path);
    return StorageID(view.database_name, fmt::format(".inner_stage.{}.{}", toString(view.uuid), path), UUID(hash.get128()));
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
    if (!cte || cte->aliases)
        throwUnsupported("WITH RECURSIVE with several CTEs or with column names");
    /// Any other query over `r` reads it as a subquery in FROM, see `inlineCTEs`.
    if (!selects_all || !reads_cte || select->distinct || select->where() || select->prewhere() || select->groupBy() || select->having()
        || select->orderBy() || select->limitBy() || select->limitLength() || select->limitOffset() || select->window() || select->qualify())
        return {};

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

/// Replaces each table in FROM of `query` that names a CTE in `scope` or in WITH by a subquery of the CTE, and so in the
/// subqueries in FROM. A recursive CTE `r` becomes `(WITH RECURSIVE r AS (...) SELECT * FROM r)`. Then removes the CTEs
/// that no clause other than FROM reads. So each CTE in FROM becomes a stage, see `DeltaQuery::Stage`.
void inlineCTEs(IAST & query, std::map<String, std::pair<ASTPtr, bool>> scope)
{
    for (const auto & child : query.as<ASTSelectWithUnionQuery &>().list_of_selects->children)
    {
        auto * select = child->as<ASTSelectQuery>();
        if (!select)
            continue;
        if (select->with())
            for (const auto & element : select->with()->children)
                if (const auto * cte = element->as<ASTWithElement>())
                    scope[cte->name] = {element, select->recursive_with};
        if (select->tables())
            for (const auto & element : select->tables()->children)
            {
                const auto & table_expression = element->as<ASTTablesInSelectQueryElement &>().table_expression;
                if (!table_expression)
                    continue;
                auto & expression = table_expression->as<ASTTableExpression &>();
                String name;
                if (const auto * identifier = expression.database_and_table_name ? expression.database_and_table_name->as<ASTTableIdentifier>() : nullptr)
                    name = identifier->name();
                else if (expression.subquery)
                    name = expression.subquery->as<ASTSubquery &>().cte_name;
                /// The body of a CTE reads a table of the name of the CTE, not the CTE.
                auto inner_scope = scope;
                if (auto cte = scope.find(name); !name.empty() && cte != scope.end())
                {
                    inner_scope.erase(name);
                    const auto alias = getTable(expression)->tryGetAlias();
                    const auto & [with_element, recursive] = cte->second;
                    auto replacement = recursive ? makeSubquery(makeRecursiveSelect(with_element->clone()))
                                                 : with_element->as<ASTWithElement &>().subquery->clone();
                    replacement->setAlias(alias.empty() ? name : alias);
                    setTable(expression, std::move(replacement));
                }
                if (expression.subquery && !getRecursiveCTE(*expression.subquery->children.at(0)))
                    inlineCTEs(*expression.subquery->children.at(0), inner_scope);
            }
        if (!select->with())
            continue;
        auto & elements = select->with()->children;
        elements.erase(std::remove_if(elements.begin(), elements.end(), [&](const ASTPtr & element)
        {
            const auto * cte = element->as<ASTWithElement>();
            return cte && std::ranges::all_of(select->children, [&](const auto & clause)
            {
                return clause == select->with() || clause == select->tables() || countTableReferences(*clause, cte->name) == 0;
            });
        }), elements.end());
        if (select->with()->children.empty())
        {
            select->setExpression(ASTSelectQuery::Expression::WITH, nullptr);
            select->recursive_with = false;
        }
    }
}

/// Whether a sorting key of MergeTree can have a column of `type`, see `KeyDescription::getSortingKeyFromAST`.
bool canBeInSortingKey(const IDataType & type)
{
    bool allowed = type.isComparable() && !dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(type.getCustomName());
    forEachInTypeTree(type, [&](const IDataType & element) { allowed = allowed && !isDynamic(element) && !isVariant(element) && !isObject(element); });
    return allowed;
}

}

bool DeltaQuery::analyzeRecursion(const ASTPtr & query_, const ContextPtr & context, const StorageID & view, const String & stage_path)
{
    auto cte = getRecursiveCTE(*query_);
    if (!cte)
        return false;
    auto split = std::make_shared<Recursion>();
    const auto & parts = cte->parts;
    auto check_part = [](const DeltaQuery & part)
    {
        if (part.residual || part.recursion)
            throwUnsupported("recursive CTEs with HAVING, ORDER BY, LIMIT, DISTINCT, windows or WITH RECURSIVE");
        if (!part.cluster.empty())
            throwUnsupported("recursive CTEs over Distributed tables");
    };
    /// The parts of a recursive stage weigh their rows like a stage.
    split->base = std::make_unique<DeltaQuery>(makeSelectWithUnion(parts[0]), context, StorageID::createEmpty(), stage_path);
    check_part(*split->base);

    /// A recursive stage reads its own stage table. The table holds `r` at the old cursor.
    String table_name = context->getSettingsRef()[Setting::delta_query_recursion_table];
    std::optional<RecursionTable> stage_table;
    ASTPtr table;
    if (is_stage)
    {
        Strings structure;
        for (const auto & column : split->base->getStageColumns())
            structure.push_back(fmt::format("{} {}", backQuote(column.name), column.type->getName()));
        stage_table = RecursionTable{.cte = cte->name, .table = makeStageTableId(view, stage_path), .structure = joinNames(structure)};
        table_name = stage_table->table.getFullTableName();
        split->key = toString(stage_table->table.uuid);
        table = make_intrusive<ASTTableIdentifier>(cte->name);
    }
    else if (!table_name.empty())
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

        auto & part = *split->steps.emplace_back(std::unique_ptr<DeltaQuery>(new DeltaQuery(makeSelectWithUnion(std::move(step)), context,
            StorageID::createEmpty(), stage_path, stage_table ? &*stage_table : nullptr)));
        check_part(part);
        auto reads_table = [&](const Input & input) { return input.key == split->key; };
        auto input = std::ranges::find_if(part.inputs, reads_table);
        if (std::ranges::count_if(part.inputs, reads_table) != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A step of the recursive CTE reads table {} of delta_query_recursion_table", table_name);
        if (!input->join_keys.empty())
            throwUnsupported("recursive CTEs whose step reads the CTE on the right of a LEFT, SEMI, ANTI or ASOF join");
        if (part.sign_column != internal_sign_column && !input->is_signed)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} of delta_query_recursion_table needs the column {} of delta_query_sign_column",
                table_name, part.sign_column);
        input->lookup = true;
    }
    recursion = std::move(split);
    return true;
}

ASTPtr DeltaQuery::analyzeStages(const ASTPtr & query_, const ContextPtr & context, const StorageID & view, const String & stage_path,
    const RecursionTable * recursion_table_)
{
    /// Each subquery in FROM becomes a stage. The analysis reads `null` with the columns of the stage table instead, so it
    /// doesn't need the stage table. The deltas read the stage table, see `makeSnapshot`.
    auto rewritten = query_->clone();
    inlineCTEs(*rewritten, {});
    for (const auto & child : rewritten->as<ASTSelectWithUnionQuery &>().list_of_selects->children)
        if (const auto * select = child->as<ASTSelectQuery>())
            forEachSubqueryInFrom(*select, [&](ASTTableExpression & expression, size_t index)
            {
                if (view.table_name.empty())
                    throwUnsupported("subqueries in FROM outside `REFRESH ... INCREMENTAL` views");
                const auto path = is_stage ? fmt::format("{}.{}", stage_path, index) : toString(index);
                auto & stage = stages.emplace_back(Stage{.path = path, .table = makeStageTableId(view, path),
                    .query = std::make_unique<DeltaQuery>(expression.subquery->children.at(0), context, view, path)});
                Strings structure;
                for (const auto & column : stage.query->getStageColumns())
                    structure.push_back(fmt::format("{} {}", backQuote(column.name), column.type->getName()));
                auto table = makeASTFunction("null", make_intrusive<ASTLiteral>(Field(joinNames(structure))));
                table->setAlias(expression.subquery->tryGetAlias());
                setTable(expression, std::move(table));
            });
    /// A step of a recursive stage reads the stage table as `null` with its columns, like a query that reads a stage.
    if (recursion_table_)
    {
        auto & select = rewritten->as<ASTSelectWithUnionQuery &>().list_of_selects->children.at(0)->as<ASTSelectQuery &>();
        auto & expression = *findTable(select, isTableNamed(recursion_table_->cte));
        auto table = makeASTFunction("null", make_intrusive<ASTLiteral>(Field(recursion_table_->structure)));
        table->setAlias(expression.database_and_table_name->tryGetAlias());
        setTable(expression, std::move(table));
        recursion_table = recursion_table_->table;
    }
    return rewritten;
}

void DeltaQuery::Recursion::checkTable() const
{
    if (steps.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Delta queries over WITH RECURSIVE need delta_query_recursion_table, the table that holds their result at the old cursor");
}

std::vector<StorageID> DeltaQuery::getStageTables(const ASTPtr & query_, const StorageID & view)
{
    std::vector<StorageID> tables;
    if (getRecursiveCTE(*query_))
        return tables;
    auto query = query_->clone();
    inlineCTEs(*query, {});
    std::function<void(const ASTPtr &, const String &)> collect = [&](const ASTPtr & select_with_union, const String & prefix)
    {
        for (const auto & child : select_with_union->as<ASTSelectWithUnionQuery &>().list_of_selects->children)
            if (const auto * select = child->as<ASTSelectQuery>())
                forEachSubqueryInFrom(*select, [&](ASTTableExpression & expression, size_t index)
                {
                    const auto path = prefix.empty() ? toString(index) : fmt::format("{}.{}", prefix, index);
                    tables.push_back(makeStageTableId(view, path));
                    if (!getRecursiveCTE(*expression.subquery->children.at(0)))
                        collect(expression.subquery->children.at(0), path);
                });
    };
    collect(query, "");
    return tables;
}

NamesAndTypesList DeltaQuery::getStageColumns() const
{
    auto columns = getResultColumns();
    for (auto & column : columns)
        if (column.name == sign_column)
            column.type = std::make_shared<DataTypeInt64>();
    return columns;
}

String DeltaQuery::getStageCreateQuery(const StorageID & table) const
{
    const auto columns = getStageColumns();
    Strings definitions;
    for (const auto & column : columns)
        definitions.push_back(fmt::format("{} {}", backQuote(column.name), column.type->getName()));
    /// The residual reads the rows of the stage table of the touched values of its partition keys.
    Names primary_key;
    if (residual)
        primary_key = residual->partition_outputs;
    /// The merges of a SummingMergeTree sum the signs of the rows that are equal in the other columns. They drop the rows of
    /// sign 0. The merges then keep the net rows below each bound that a delta query holds, see `checkReadableAsOf`.
    const bool consolidates = retractsRows() && std::ranges::any_of(columns, [&](const auto & column) { return column.name == sign_column; })
        && std::ranges::all_of(columns, [&](const auto & column) { return column.name == sign_column || canBeInSortingKey(*column.type); });
    Names order = primary_key;
    if (consolidates)
        for (const auto & column : columns)
            if (column.name != sign_column && std::ranges::find(primary_key, column.name) == primary_key.end())
                order.push_back(column.name);
    auto key = [](const Names & names)
    {
        Strings quoted;
        for (const auto & name : names)
            quoted.push_back(backQuote(name));
        return names.empty() ? String("tuple()") : "(" + joinNames(quoted) + ")";
    };
    /// The deduplication log drops the blocks that the retry of a refresh inserts again.
    return fmt::format("CREATE TABLE IF NOT EXISTS {} UUID {} ({}) ENGINE = {} ORDER BY {}{} "
        "SETTINGS enable_block_number_column = 1, allow_nullable_key = 1, non_replicated_deduplication_window = 100",
        table.getFullTableName(), quoteString(toString(table.uuid)),
        joinNames(definitions), consolidates ? "SummingMergeTree(" + backQuote(sign_column) + ")" : "MergeTree", key(order),
        consolidates && !primary_key.empty() ? " PRIMARY KEY " + key(primary_key) : "");
}

void DeltaQuery::bindStageTables(const ContextPtr & context)
{
    if (recursion)
    {
        recursion->base->bindStageTables(context);
        for (const auto & step : recursion->steps)
            step->bindStageTables(context);
    }
    for (auto & input : inputs)
        if (input.stage)
            input.storage = castStorage<MergeTreeData>(DatabaseCatalog::instance().getTable(input.table_id, context), DeferredTable::Load);
}

/// `WITH RECURSIVE d AS (SELECT * FROM (d_0) UNION ALL step(d) ...) SELECT * FROM d`. The delta `d_0` is the delta of the
/// base plus the delta of each step. The steps read all rows of the table that holds `r` at the old cursor.
ASTPtr DeltaQuery::buildRecursiveDelta(const CursorTreeNodePtr & old_cursor, const CursorTreeNodePtr & new_cursor) const
{
    recursion->checkTable();
    constexpr auto cte = "__recursion";
    const auto & base = *recursion->base;
    ASTs anchors{base.buildDelta(base.restrictCursor(old_cursor), base.restrictCursor(new_cursor))};
    ASTs terms;
    for (const auto & step : recursion->steps)
    {
        auto step_cursor = new_cursor ? step->restrictCursor(new_cursor) : std::make_shared<CursorTreeNode>();
        if (auto frontier = step->getFrontier(nullptr); frontier->hasSubtree(recursion->key))
            step_cursor->setSubtree(recursion->key, frontier->getSubtree(recursion->key));
        anchors.push_back(step->buildDelta(step->restrictCursor(old_cursor), step_cursor));
        terms.push_back(step->buildRecursiveTerm(step_cursor, recursion->key, cte));
    }
    terms.insert(terms.begin(), makeSelect({make_intrusive<ASTAsterisk>()}, makeSubquery(makeUnionAll(anchors))));
    auto element = make_intrusive<ASTWithElement>();
    element->name = cte;
    element->subquery = makeSubquery(makeUnionAll(terms));
    element->children.push_back(element->subquery);
    return makeRecursiveSelect(std::move(element));
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
        addBounds(conjuncts, input.alias, nullptr, bounds.at(input.key), nullptr);
        setInput(term_select, input, makeSnapshot(input, bounds.at(input.key)), std::move(conjuncts), where, sign_column);
    }
    term_select.setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
    bindTerm(*term, 1, getTime(new_cursor));
    return term;
}

}
