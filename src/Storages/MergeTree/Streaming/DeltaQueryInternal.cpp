#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Analyzer/FunctionNode.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/addTypeConversionToAST.h>
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
#include <Parsers/ASTWithElement.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>

#include <fmt/ranges.h>

#include <map>
#include <set>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TABLE;
}

namespace DeltaQueryInternal
{

std::shared_ptr<MergeTreeData> lockStorage(const DeltaQuery::Input & input)
{
    auto storage = input.storage.lock();
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} of the delta query was dropped", input.table_id.getNameForLogs());
    return storage;
}

[[noreturn]] void throwUnsupported(std::string_view construct)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Delta queries do not support {}", construct);
}

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

bool isSum(const String & name)
{
    const auto [base, suffixes] = splitCombinators(name);
    return (base == "sum" || base == "count") && (suffixes.empty() || suffixes == std::vector<String>{"If"});
}

bool isInvertibleAggregate(const String & name)
{
    if (isSum(name))
        return true;
    const auto [base, suffixes] = splitCombinators(name);
    return base == "exponentialMovingAverage" && suffixes == std::vector<String>{"State"};
}

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

bool isAggregate(const IAST & ast)
{
    const auto * function = ast.as<ASTFunction>();
    return function && !function->isWindowFunction() && AggregateFunctionFactory::instance().isAggregateFunctionName(function->name);
}

bool hasAggregates(const IAST & ast)
{
    return isAggregate(ast) || std::ranges::any_of(ast.children, [](const auto & child) { return hasAggregates(*child); });
}

ASTPtr & getTable(ASTTableExpression & expression)
{
    if (expression.table_function)
        return expression.table_function;
    if (expression.subquery)
        return expression.subquery;
    return expression.database_and_table_name;
}

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

ASTTableExpression & findAliasedTable(const ASTSelectQuery & select, const String & alias)
{
    auto * expression = findTable(select, [&](const IAST & table) { return table.tryGetAlias() == alias; });
    chassert(expression);
    return *expression;
}

void setTable(ASTTableExpression & expression, ASTPtr table)
{
    expression.reset(getTable(expression));
    auto & field = table->as<ASTTableIdentifier>() ? expression.database_and_table_name
        : table->as<ASTFunction>() ? expression.table_function : expression.subquery;
    expression.setOrReplace(field, std::move(table));
}

std::function<bool(const IAST & table)> isTableNamed(const String & name)
{
    return [name](const IAST & table)
    {
        const auto * identifier = table.as<ASTTableIdentifier>();
        return identifier && identifier->name() == name;
    };
}

size_t countTableReferences(const IAST & ast, const String & name)
{
    const auto * expression = ast.as<ASTTableExpression>();
    size_t count = expression && expression->database_and_table_name && isTableNamed(name)(*expression->database_and_table_name) ? 1 : 0;
    for (const auto & child : ast.children)
        count += countTableReferences(*child, name);
    return count;
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

ASTPtr parseSelect(const String & sql)
{
    ParserSelectWithUnionQuery parser;
    auto ast = parseQuery(parser, sql, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    NormalizeSelectWithUnionQueryVisitor::Data data{SetOperationMode::ALL};
    NormalizeSelectWithUnionQueryVisitor(data).visit(ast);
    return ast;
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

ASTPtr makeUnionAll(const ASTs & queries)
{
    auto selects = make_intrusive<ASTExpressionList>();
    for (const auto & query : queries)
    {
        if (const auto * union_query = query->as<ASTSelectWithUnionQuery>())
        {
            const auto & children = union_query->list_of_selects->children;
            chassert(children.size() == 1 || union_query->union_mode == SelectUnionMode::UNION_ALL);
            selects->children.insert(selects->children.end(), children.begin(), children.end());
        }
        else
            selects->children.push_back(query);
    }
    if (selects->children.size() == 1)
        return makeSelectWithUnion(selects->children[0]);
    auto result = make_intrusive<ASTSelectWithUnionQuery>();
    result->union_mode = SelectUnionMode::UNION_ALL;
    result->is_normalized = true;
    result->list_of_selects = selects;
    result->children.push_back(std::move(selects));
    return result;
}

ASTPtr makeSubquery(ASTPtr query)
{
    if (query->as<ASTSelectQuery>())
        query = makeSelectWithUnion(std::move(query));
    return make_intrusive<ASTSubquery>(std::move(query));
}

ASTPtr makeTables(ASTPtr table)
{
    auto expression = make_intrusive<ASTTableExpression>();
    setTable(*expression, std::move(table));
    auto element = make_intrusive<ASTTablesInSelectQueryElement>();
    element->table_expression = expression;
    element->children.push_back(std::move(expression));
    auto tables = make_intrusive<ASTTablesInSelectQuery>();
    tables->children.push_back(std::move(element));
    return tables;
}

ASTPtr makeSelect(ASTs columns, ASTPtr table, ASTPtr where, ASTs group_by, ASTPtr having)
{
    auto select = make_intrusive<ASTSelectQuery>();
    auto list = make_intrusive<ASTExpressionList>();
    list->children = std::move(columns);
    select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(list));
    if (table)
        select->setExpression(ASTSelectQuery::Expression::TABLES, makeTables(std::move(table)));
    select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));
    if (!group_by.empty())
    {
        auto keys = make_intrusive<ASTExpressionList>();
        keys->children = std::move(group_by);
        select->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(keys));
    }
    select->setExpression(ASTSelectQuery::Expression::HAVING, std::move(having));
    return select;
}

void addArrayJoin(ASTSelectQuery & select, ASTPtr expression)
{
    auto array_join = make_intrusive<ASTArrayJoin>();
    array_join->kind = ASTArrayJoin::Kind::Inner;
    array_join->expression_list = make_intrusive<ASTExpressionList>();
    array_join->expression_list->children.push_back(std::move(expression));
    array_join->children.push_back(array_join->expression_list);
    auto element = make_intrusive<ASTTablesInSelectQueryElement>();
    element->array_join = array_join;
    element->children.push_back(std::move(array_join));
    select.tables()->children.push_back(std::move(element));
}

ASTPtr makeRecursiveSelect(ASTPtr element)
{
    auto select = makeSelect({make_intrusive<ASTAsterisk>()}, make_intrusive<ASTTableIdentifier>(element->as<ASTWithElement &>().name));
    auto & select_query = select->as<ASTSelectQuery &>();
    auto with = make_intrusive<ASTExpressionList>();
    with->children.push_back(std::move(element));
    select_query.setExpression(ASTSelectQuery::Expression::WITH, std::move(with));
    select_query.recursive_with = true;
    return makeSelectWithUnion(std::move(select));
}

ASTPtr withAlias(ASTPtr ast, const String & alias)
{
    ast->setAlias(alias);
    return ast;
}

ASTs makeIdentifiers(const Names & names)
{
    ASTs identifiers;
    for (const auto & name : names)
        identifiers.push_back(make_intrusive<ASTIdentifier>(name));
    return identifiers;
}

ASTPtr makeColumn(const String & qualifier, const String & column)
{
    if (qualifier.empty())
        return make_intrusive<ASTIdentifier>(column);
    return make_intrusive<ASTIdentifier>(std::vector<String>{qualifier, column});
}

ASTPtr makeTuple(ASTs elements)
{
    if (elements.size() == 1)
        return std::move(elements[0]);
    return makeASTFunction("tuple", std::move(elements));
}

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

void collectNames(const IAST & ast, NameSet & names)
{
    if (const auto * identifier = ast.as<ASTIdentifier>())
        names.insert(identifier->name_parts.begin(), identifier->name_parts.end());
    if (const auto & alias = ast.tryGetAlias(); !alias.empty())
        names.insert(alias);
    for (const auto & child : ast.children)
        collectNames(*child, names);
}

String makeUniqueName(const String & base, const NameSet & taken)
{
    String name = base;
    for (size_t i = 1; taken.contains(name); ++i)
        name = fmt::format("{}_{}", base, i);
    return name;
}

void bindTerm(IAST & ast, std::optional<Int64> sign, std::optional<Int64> time)
{
    for (auto & child : ast.children)
    {
        const auto * identifier = child->as<ASTIdentifier>();
        const auto * function = child->as<ASTFunction>();
        if (sign && identifier && identifier->name() == term_sign_name)
        {
            auto literal = make_intrusive<ASTLiteral>(Field(*sign));
            literal->setAlias(identifier->tryGetAlias());
            child = std::move(literal);
        }
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

void collectColumns(const IAST & ast, std::map<String, std::set<String>> & columns)
{
    if (const auto * identifier = ast.as<ASTIdentifier>(); identifier && identifier->compound())
        columns[identifier->name_parts[0]].insert(identifier->name_parts[1]);
    for (const auto & child : ast.children)
        collectColumns(*child, columns);
}

void setSetting(ASTSelectQuery & select, std::string_view name, const Field & value)
{
    auto settings = select.settings() ? select.settings()->clone() : make_intrusive<ASTSetQuery>();
    auto & set_query = settings->as<ASTSetQuery &>();
    set_query.is_standalone = false;
    set_query.changes.setSetting(name, value);
    select.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));
}

using Visit = std::function<ASTPtr(const ASTPtr & node)>;

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

String joinNames(const Strings & names)
{
    return fmt::format("{}", fmt::join(names, ", "));
}

ASTPtr castTo(const String & column, const NamesAndTypesList & types)
{
    return withAlias(addTypeConversionToAST(make_intrusive<ASTIdentifier>(column), types.tryGetByName(column)->type->getName()), column);
}

ASTPtr consolidate(const Names & columns, const String & sign, std::vector<WeightedRows> sources, bool empty)
{
    /// The weight and the count need names that no column has.
    NameSet taken(columns.begin(), columns.end());
    taken.insert(sign);
    const auto weight = makeUniqueName("__weight", taken);
    taken.insert(weight);
    const auto count = makeUniqueName("__count", taken);
    auto count_column = [&] { return make_intrusive<ASTIdentifier>(count); };

    ASTs weighted;
    for (auto & source : sources)
    {
        source.values.push_back(withAlias(std::move(source.weight), weight));
        weighted.push_back(makeSelect(std::move(source.values), std::move(source.table), std::move(source.where)));
    }
    auto sums = makeIdentifiers(columns);
    sums.push_back(withAlias(makeASTFunction("sum", make_intrusive<ASTIdentifier>(weight)), count));
    auto summed = makeSelect(std::move(sums), makeSubquery(makeUnionAll(weighted)), nullptr, makeIdentifiers(columns),
        makeASTFunction("notEquals", count_column(), make_intrusive<ASTLiteral>(Field(UInt64{0}))));

    auto rows = makeIdentifiers(columns);
    rows.push_back(withAlias(makeASTFunction("arrayJoin", makeASTFunction("arrayWithConstant",
        makeASTFunction("toUInt64", makeASTFunction("abs", count_column())), makeASTFunction("toInt8", makeASTFunction("sign", count_column())))), sign));
    auto result = makeSelect(std::move(rows), makeSubquery(std::move(summed)), empty ? make_intrusive<ASTLiteral>(Field(UInt8{0})) : nullptr);
    auto order_by = make_intrusive<ASTOrderByElement>();
    order_by->direction = 1;
    order_by->nulls_direction = 1;
    order_by->children.push_back(make_intrusive<ASTIdentifier>(sign));
    auto order_by_list = make_intrusive<ASTExpressionList>();
    order_by_list->children.push_back(std::move(order_by));
    result->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_list));
    return makeSelectWithUnion(std::move(result));
}

}

}
