#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <random>
#include <tuple>

namespace DB
{

namespace
{

/// A row of a table with `ORDER BY (a, b)` and a column `c` that is not in the key.
struct Row
{
    Int64 a;
    Int64 b;
    Int64 c;
};

/// A random filter over `a`, `b`, and `c`, kept both as a tree to evaluate on rows and as an `ActionsDAG` node.
struct Filter
{
    std::function<bool(const Row &)> evaluate;
    const ActionsDAG::Node * node;
};

class FilterGenerator
{
public:
    FilterGenerator(ContextPtr context_, ActionsDAG & dag_, std::mt19937 & rng_) : context(context_), dag(dag_), rng(rng_)
    {
        for (const char * name : {"a", "b", "c"})
            inputs.push_back(&dag.addInput(name, std::make_shared<DataTypeInt64>()));
    }

    Filter generate(size_t depth)
    {
        size_t kind = std::uniform_int_distribution<size_t>(0, depth == 0 ? 2 : 5)(rng);
        if (kind <= 2)
            return atom();
        if (kind == 3)
        {
            Filter arg = generate(depth - 1);
            return {[f = arg.evaluate](const Row & row) { return !f(row); }, &function("not", {arg.node})};
        }
        Filter left = generate(depth - 1);
        Filter right = generate(depth - 1);
        if (kind == 4)
            return {[l = left.evaluate, r = right.evaluate](const Row & row) { return l(row) && r(row); }, &function("and", {left.node, right.node})};
        return {[l = left.evaluate, r = right.evaluate](const Row & row) { return l(row) || r(row); }, &function("or", {left.node, right.node})};
    }

private:
    Filter atom()
    {
        size_t column = std::uniform_int_distribution<size_t>(0, 2)(rng);
        Int64 value = std::uniform_int_distribution<Int64>(0, 4)(rng);
        const auto & constant = dag.addColumn(
            {DataTypeInt64().createColumnConst(1, value), std::make_shared<DataTypeInt64>(), fmt::format("{}_{}", value, counter++)});
        auto get = [column](const Row & row) { return column == 0 ? row.a : (column == 1 ? row.b : row.c); };

        switch (std::uniform_int_distribution<int>(0, 2)(rng))
        {
            case 0:
                return {[get, value](const Row & row) { return get(row) < value; }, &function("less", {inputs[column], &constant})};
            case 1:
                return {[get, value](const Row & row) { return get(row) == value; }, &function("equals", {inputs[column], &constant})};
            default:
                return {[get, value](const Row & row) { return get(row) > value; }, &function("greater", {inputs[column], &constant})};
        }
    }

    const ActionsDAG::Node & function(const String & name, ActionsDAG::NodeRawConstPtrs args)
    {
        ColumnsWithTypeAndName arguments;
        for (const auto * arg : args)
            arguments.emplace_back(arg->column, arg->result_type, arg->result_name);
        auto function_base = FunctionFactory::instance().get(name, context)->build(arguments);
        return dag.addFunction(function_base, std::move(args), fmt::format("{}_{}", name, counter++));
    }

    ContextPtr context;
    ActionsDAG & dag;
    std::mt19937 & rng;
    ActionsDAG::NodeRawConstPtrs inputs;
    size_t counter = 0;
};

constexpr size_t rows_per_mark = 4;
constexpr size_t num_marks = 8;

/// The mask of the filter on the rows of marks [begin, end).
BoolMask maskOnRows(const Filter & filter, const std::vector<Row> & rows, size_t begin, size_t end)
{
    BoolMask mask;
    for (size_t i = begin * rows_per_mark; i < end * rows_per_mark; ++i)
    {
        bool value = filter.evaluate(rows[i]);
        mask.can_be_true |= value;
        mask.can_be_false |= !value;
    }
    return mask;
}

}

/// `checkBoundsInRange` on the key range of a mark range, the way the primary key analysis calls it, against the filter
/// evaluated on the rows, which are never NULL. Each side of the truth of a range allows the rows of the range, and a
/// subrange is `Never` or `Always` on a side only if the range says it can be.
TEST(KeyConditionBounds, BoundsHoldOnRandomFilters)
{
    auto context = getRegisteredContext();
    std::mt19937 rng(42);
    const DataTypes key_types{std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};

    for (size_t iteration = 0; iteration < 300; ++iteration)
    {
        std::vector<Row> rows(rows_per_mark * num_marks);
        for (auto & row : rows)
            row = {std::uniform_int_distribution<Int64>(0, 4)(rng), std::uniform_int_distribution<Int64>(0, 4)(rng),
                   std::uniform_int_distribution<Int64>(0, 4)(rng)};
        std::ranges::sort(rows, [](const Row & l, const Row & r) { return std::tie(l.a, l.b) < std::tie(r.a, r.b); });

        ActionsDAG dag;
        FilterGenerator generator(context, dag, rng);
        Filter filter = generator.generate(3);
        dag.getOutputs().push_back(filter.node);

        ActionsDAG key_dag;
        key_dag.getOutputs().push_back(&key_dag.addInput("a", key_types[0]));
        key_dag.getOutputs().push_back(&key_dag.addInput("b", key_types[1]));

        ActionsDAGWithInversionPushDown filter_dag(filter.node, context, /*boolean_context=*/ true);
        KeyCondition condition(filter_dag, context, Names{"a", "b"}, std::make_shared<ExpressionActions>(std::move(key_dag)));

        /// The key range of marks [begin, end) runs from the first row of `begin` to the first row of `end`, or to the last
        /// row for the last mark.
        auto truth = [&](size_t begin, size_t end)
        {
            const Row & left = rows[begin * rows_per_mark];
            const Row & right = rows[std::min(end * rows_per_mark, rows.size() - 1)];
            std::vector<FieldRef> left_keys{Field(left.a), Field(left.b)};
            std::vector<FieldRef> right_keys{Field(right.a), Field(right.b)};
            return condition.checkBoundsInRange(2, left_keys.data(), right_keys.data(), key_types);
        };

        for (size_t begin = 0; begin < num_marks; ++begin)
        {
            for (size_t end = begin + 1; end <= num_marks; ++end)
            {
                SCOPED_TRACE(fmt::format("condition {}, marks [{}, {})", condition.toString(), begin, end));
                const Truth range_truth = truth(begin, end);
                const BoolMask mask = maskOnRows(filter, rows, begin, end);
                const std::vector<std::pair<Side, BoolMask>> sides{
                    {range_truth.is_true, mask}, {range_truth.is_false, !mask}, {range_truth.is_null, BoolMask(false, true)}};
                for (const auto & [side, rows_mask] : sides)
                {
                    EXPECT_TRUE(!rows_mask.can_be_true || side.level() != Side::Never);
                    EXPECT_TRUE(!rows_mask.can_be_false || side.level() != Side::Always);
                }

                for (size_t sub_begin = begin; sub_begin < end; ++sub_begin)
                {
                    for (size_t sub_end = sub_begin + 1; sub_end <= end; ++sub_end)
                    {
                        const Truth sub_truth = truth(sub_begin, sub_end);
                        const std::vector<std::pair<Side, Side>> sub_sides{
                            {sub_truth.is_true, range_truth.is_true},
                            {sub_truth.is_false, range_truth.is_false},
                            {sub_truth.is_null, range_truth.is_null}};
                        for (const auto & [sub_side, side] : sub_sides)
                        {
                            if (sub_side.level() == Side::Never)
                                EXPECT_TRUE(side.mayBeNever());
                            if (sub_side.level() == Side::Always)
                                EXPECT_TRUE(side.mayBeAlways());
                        }
                    }
                }
            }
        }
    }
}

}
