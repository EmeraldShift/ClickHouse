#include <Storages/MergeTree/Streaming/DeltaQueryInternal.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <set>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace DeltaQueryInternal
{

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

bool isInvertibleAggregate(const String & name)
{
    const auto [base, suffixes] = splitCombinators(name);
    if (base == "sum" || base == "count")
        return suffixes.empty() || suffixes == std::vector<String>{"If"};
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

ASTPtr makeColumn(const String & qualifier, const String & column)
{
    if (qualifier.empty())
        return make_intrusive<ASTIdentifier>(column);
    return make_intrusive<ASTIdentifier>(std::vector<String>{qualifier, column});
}

ASTPtr makeConjunction(ASTs conjuncts)
{
    if (conjuncts.empty())
        return make_intrusive<ASTLiteral>(Field(UInt8{1}));
    if (conjuncts.size() == 1)
        return std::move(conjuncts[0]);
    return makeASTFunction("and", std::move(conjuncts));
}

void setSetting(ASTSelectQuery & select, std::string_view name, const Field & value)
{
    auto settings = select.settings() ? select.settings()->clone() : make_intrusive<ASTSetQuery>();
    auto & set_query = settings->as<ASTSetQuery &>();
    set_query.is_standalone = false;
    set_query.changes.setSetting(name, value);
    select.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings));
}

}

}
