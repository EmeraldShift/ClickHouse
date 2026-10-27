#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/addTypeConversionToAST.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTWindowDefinition.h>

#include <map>
#include <set>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

using namespace DeltaQueryInternal;

namespace
{

String makeKeyName(size_t key) { return fmt::format("__key_{}", key + 1); }

String makeValueName(size_t value) { return fmt::format("__val_{}", value + 1); }

/// `__count > 0`.
ASTPtr makePositiveCount()
{
    return makeASTFunction("greater", make_intrusive<ASTIdentifier>("__count"), make_intrusive<ASTLiteral>(Field(UInt64{0})));
}

/// `ARRAY JOIN range(toUInt64(count)) AS __index`, which repeats each row of `select` `count` times.
void repeatRows(ASTSelectQuery & select, const String & count)
{
    addArrayJoin(select, withAlias(makeASTFunction("range", makeASTFunction("toUInt64", make_intrusive<ASTIdentifier>(count))), "__index"));
}

/// Whether the core, the residual or the delta of the residual names its own columns `name`.
bool isGeneratedName(std::string_view name)
{
    for (std::string_view prefix : {"__key_", "__val_", "__agg_"})
        if (name.starts_with(prefix))
            return true;
    return name == "__count" || name == "__index";
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

namespace DeltaQueryInternal
{

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
        return replace_by(makeValueName(value->second), node);
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

}

void DeltaQuery::analyzeResidual(Residual split, const ASTPtr & core, const ContextPtr & context)
{
    analyze(core, context, split.sign_column, nullptr, /*reads_linearly=*/ {});
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
        analyze(core, context, split.sign_column, nullptr, /*reads_linearly=*/ {});
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
    for (auto & merge : split.merges)
        castColumns(*merge, simple_types);

    /// The residual over an empty integral has the types of the rows that the residual deltas insert.
    auto empty_integral = makeASTFunction("null", make_intrusive<ASTLiteral>(Field(joinNames(structure))));
    split.output_query = makeSelectWithUnion(makeSelect(
        {make_intrusive<ASTAsterisk>(), withAlias(makeASTFunction("toInt8", make_intrusive<ASTLiteral>(Field(UInt64{1}))), split.sign_column)},
        makeSubquery(split.buildResidual(split.buildInput(std::move(empty_integral), nullptr)))));
    split.output_columns = InterpreterSelectQueryAnalyzer::getSampleBlock(split.output_query, context)->getNamesAndTypesList();
    for (const auto & column : split.output_columns)
        if (WhichDataType(column.type).isAggregateFunction())
            throwUnsupported(fmt::format("aggregate states like column {} in the results of queries with HAVING, ORDER BY, LIMIT, DISTINCT, windows or expressions over aggregates", column.name));
    residual = std::make_shared<const Residual>(std::move(split));
}

ASTPtr DeltaQuery::Residual::buildInput(ASTPtr integral, ASTPtr where) const
{
    Names keys;
    for (size_t i = 0; i < num_keys; ++i)
        keys.push_back(makeKeyName(i));
    auto columns = makeIdentifiers(keys);
    auto count = withAlias(makeASTFunction("sum", make_intrusive<ASTIdentifier>("__agg_0")), "__count");
    if (!has_aggregates)
    {
        /// A row of the integral with count n stands for n rows.
        columns.push_back(std::move(count));
        auto expanded = makeIdentifiers(keys);
        expanded.push_back(make_intrusive<ASTIdentifier>("__index"));
        auto result = makeSelect(std::move(expanded), makeSubquery(makeSelect(std::move(columns), std::move(integral), std::move(where),
            makeIdentifiers(keys), makePositiveCount())));
        repeatRows(result->as<ASTSelectQuery &>(), "__count");
        return result;
    }
    /// A group whose rows were all retracted has count 0. A query without GROUP BY returns its one row anyway.
    for (size_t i = 0; i < merges.size(); ++i)
        columns.push_back(withAlias(merges[i]->clone(), makeValueName(i)));
    columns.push_back(std::move(count));
    return makeSelect(std::move(columns), std::move(integral), std::move(where), makeIdentifiers(keys), keys.empty() ? nullptr : makePositiveCount());
}

ASTPtr DeltaQuery::Residual::buildAppendOnlyInput(const StorageID & target, ASTPtr where, const StorageID & delta) const
{
    ASTs state;
    Names keys;
    for (size_t i = 0; i < num_keys; ++i)
    {
        /// The target can declare another type than the key, like UInt64 for a key of type Int64.
        keys.push_back(makeKeyName(i));
        state.push_back(withAlias(addTypeConversionToAST(make_intrusive<ASTIdentifier>(key_outputs.at(i)), key_types.at(i)->getName()), keys.back()));
    }
    /// A row of the target with signs that sum to n stands for n rows, and so does a row of the delta with count n.
    auto target_rows = makeIdentifiers(outputs);
    target_rows.push_back(withAlias(makeASTFunction("sum", make_intrusive<ASTIdentifier>(sign_column)), "__count"));
    auto from_target = makeSelect(std::move(state), makeSubquery(makeSelect(std::move(target_rows), make_intrusive<ASTTableIdentifier>(target),
        std::move(where), makeIdentifiers(outputs), makePositiveCount())));
    repeatRows(from_target->as<ASTSelectQuery &>(), "__count");
    auto from_delta = makeSelect(makeIdentifiers(keys), make_intrusive<ASTTableIdentifier>(delta));
    repeatRows(from_delta->as<ASTSelectQuery &>(), "__agg_0");
    return makeUnionAll({std::move(from_target), std::move(from_delta)});
}

ASTPtr DeltaQuery::Residual::buildResidual(ASTPtr input) const
{
    auto result = outer->clone();
    result->as<ASTSelectQuery &>().setExpression(ASTSelectQuery::Expression::TABLES, makeTables(makeSubquery(std::move(input))));
    return result;
}

bool DeltaQuery::isAppendOnly() const
{
    return residual && residual->append_only;
}

ASTPtr DeltaQuery::Residual::buildPartitionFilter(const Names & columns, const StorageID & delta) const
{
    /// `IN` drops NULLs, so a nullable key compares as whether it is NULL and its value otherwise.
    ASTs left;
    ASTs right;
    for (size_t i = 0; i < partition_keys.size(); ++i)
    {
        const bool nullable = isNullableOrLowCardinalityNullable(key_types.at(partition_keys[i]));
        auto add = [&](ASTs & values, const String & name)
        {
            auto column = make_intrusive<ASTIdentifier>(name);
            if (nullable)
                values.insert(values.end(), {makeASTFunction("isNull", column->clone()), makeASTFunction("assumeNotNull", column)});
            else
                values.push_back(column);
        };
        add(left, columns.at(i));
        add(right, makeKeyName(partition_keys[i]));
    }
    return makeASTFunction("in", makeTuple(std::move(left)), makeSubquery(makeSelect(std::move(right), make_intrusive<ASTTableIdentifier>(delta))));
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
    return fmt::format("CREATE TABLE IF NOT EXISTS {} UUID {} ({}) ENGINE = AggregatingMergeTree ORDER BY {} "
        "SETTINGS allow_nullable_key = 1, non_replicated_deduplication_window = 100",
        integral.getFullTableName(), quoteString(toString(integral.uuid)), joinNames(columns), order.empty() ? "tuple()" : "(" + joinNames(order) + ")");
}

/// The residual over the touched keys counts each of its rows once, and the target counts each of its rows of those keys by
/// the sum of their signs. The difference of the counts per row is the delta, which becomes that many rows of sign 1 or -1.
ASTPtr DeltaQuery::buildResidualDelta(const StorageID & integral, const StorageID & delta, bool delta_is_empty, const StorageID & target,
    bool all_keys) const
{
    chassert(residual);
    ASTPtr where;
    ASTPtr target_where;
    if (!all_keys && !residual->partition_keys.empty())
    {
        Names keys;
        for (auto key : residual->partition_keys)
            keys.push_back(makeKeyName(key));
        where = residual->buildPartitionFilter(keys, delta);
        target_where = residual->buildPartitionFilter(residual->partition_outputs, delta);
    }
    /// An empty delta leaves the residual unchanged, but a global aggregate has a row before its first delta.
    const bool skip = delta_is_empty && !(residual->has_aggregates && residual->num_keys == 0);
    auto input = residual->append_only
        ? residual->buildAppendOnlyInput(target, target_where ? target_where->clone() : nullptr, delta)
        : residual->buildInput(make_intrusive<ASTTableIdentifier>(integral), std::move(where));
    const auto & outputs = residual->outputs;
    return consolidate(outputs, residual->sign_column, {
        {makeSubquery(residual->buildResidual(std::move(input))), makeIdentifiers(outputs),
            makeASTFunction("toInt64", make_intrusive<ASTLiteral>(Field(UInt64{1})))},
        {make_intrusive<ASTTableIdentifier>(target), castToResultTypes(outputs),
            makeASTFunction("negate", makeASTFunction("toInt64", make_intrusive<ASTIdentifier>(residual->sign_column))), std::move(target_where)}},
        skip);
}

bool DeltaQuery::hasPartitionedResidual() const
{
    return residual && !residual->partition_keys.empty();
}

}
