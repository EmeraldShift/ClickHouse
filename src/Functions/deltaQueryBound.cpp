#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/Streaming/DeltaQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// `__deltaQueryBound('slot', _partition_id)`, the block number that slot `slot` of the bounds of a delta has for the partition
/// of each row, or 0 for a partition without one. The bounds come from the context that builds the function, see
/// `DeltaQuery::buildDelta`. So the text and the plan of a delta stay the same when only its cursors change.
class FunctionDeltaQueryBound final : public IFunction
{
public:
    static constexpr auto name = "__deltaQueryBound";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionDeltaQueryBound>(context->getDeltaQueryBounds()); }

    explicit FunctionDeltaQueryBound(std::shared_ptr<const DeltaQueryBounds> bounds_) : bounds(std::move(bounds_)) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return true; }

    /// Folding would put the bounds of one execution into the plan.
    bool isSuitableForConstantFolding() const override { return false; }
    /// The query condition cache and the query cache identify a filter by its text, which doesn't have the bounds.
    bool isDeterministic() const override { return false; }
    /// The part pruning by virtual columns and the filter push-down need this.
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]) || !isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} takes a slot and a partition ID", getName());
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto slot = arguments[0].column->getDataAt(0);
        if (!bounds || !bounds->slots.contains(slot))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} has no bounds for slot {}", getName(), slot);
        const auto & partitions = bounds->slots.find(slot)->second;
        const auto & partition_ids = checkAndGetColumn<ColumnString>(*arguments[1].column);
        auto result = ColumnInt64::create(input_rows_count);
        auto & data = result->getData();
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto partition = partitions.find(partition_ids.getDataAt(i));
            data[i] = partition == partitions.end() ? 0 : partition->second;
        }
        return result;
    }

private:
    std::shared_ptr<const DeltaQueryBounds> bounds;
};

}

REGISTER_FUNCTION(DeltaQueryBound)
{
    factory.registerFunction<FunctionDeltaQueryBound>(FunctionDocumentation::INTERNAL_FUNCTION_DOCS);
}

}
