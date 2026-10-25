#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>


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

}

bool DeltaQuery::analyzeRecursion(const ASTPtr & query_, const ContextPtr & context)
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
        if (part.sign_column != internal_sign_column && !input->is_signed)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} of delta_query_recursion_table needs the column {} of delta_query_sign_column",
                table_name, part.sign_column);
        input->lookup = true;
    }
    recursion = std::move(split);
    return true;
}

void DeltaQuery::Recursion::checkTable() const
{
    if (steps.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Delta queries over WITH RECURSIVE need delta_query_recursion_table, the table that holds their result at the old cursor");
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
        addBounds(conjuncts, input.alias, nullptr, bounds.at(input.key));
        setInput(term_select, input, makeSnapshot(input, bounds.at(input.key)), std::move(conjuncts), where, sign_column);
    }
    term_select.setExpression(ASTSelectQuery::Expression::WHERE, makeConjunction(std::move(where)));
    bindTerm(*term, 1, getTime(new_cursor));
    return term;
}

}
