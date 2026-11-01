#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <algorithm>
#include <functional>
#include <random>

using namespace DB;

namespace
{

/// A row of a table with `ORDER BY (a, b)`. The key doesn't store `c`.
using TableRow = std::array<Int64, 3>;

/// A random filter over `a`, `b`, and `c`, both as an `ActionsDAG` node and as a function of a row.
struct Filter
{
    const ActionsDAG::Node * node;
    std::function<bool(const TableRow &)> evaluate;
};

Filter randomFilter(ActionsDAG & dag, const ActionsDAG::NodeRawConstPtrs & inputs, ContextPtr context, std::mt19937 & rng, size_t depth)
{
    auto function = [&](const String & name, ActionsDAG::NodeRawConstPtrs args) -> const ActionsDAG::Node &
    {
        ColumnsWithTypeAndName arguments;
        for (const auto * arg : args)
            arguments.emplace_back(arg->column, arg->result_type, arg->result_name);
        auto function_base = FunctionFactory::instance().get(name, context)->build(arguments);
        return dag.addFunction(function_base, std::move(args), fmt::format("{}_{}", name, dag.getNodes().size()));
    };

    const size_t kind = std::uniform_int_distribution<size_t>(0, depth == 0 ? 2 : 5)(rng);
    if (kind <= 2)
    {
        const size_t column = std::uniform_int_distribution<size_t>(0, 2)(rng);
        const Int64 value = std::uniform_int_distribution<Int64>(0, 4)(rng);
        const auto & constant = dag.addColumn(
            DataTypeInt64().createColumnConst(1, value), std::make_shared<DataTypeInt64>(), fmt::format("c_{}", dag.getNodes().size()));
        static const std::array<const char *, 3> names{"less", "equals", "greater"};
        return {&function(names[kind], {inputs[column], &constant}), [column, value, kind](const TableRow & row)
                {
                    return kind == 0 ? row[column] < value : (kind == 1 ? row[column] == value : row[column] > value);
                }};
    }
    Filter left = randomFilter(dag, inputs, context, rng, depth - 1);
    if (kind == 3)
        return {&function("not", {left.node}), [l = left.evaluate](const TableRow & row) { return !l(row); }};
    Filter right = randomFilter(dag, inputs, context, rng, depth - 1);
    if (kind == 4)
        return {&function("and", {left.node, right.node}), [l = left.evaluate, r = right.evaluate](const TableRow & row) { return l(row) && r(row); }};
    return {&function("or", {left.node, right.node}), [l = left.evaluate, r = right.evaluate](const TableRow & row) { return l(row) || r(row); }};
}

}

/// Checks `checkBoundsInRange` on the key range of a mark range against the filter on the rows. Each side of the truth
/// of a range must hold on its rows. A subrange may be at `Never` on a side only if `lo` of the range is `Never`, and at
/// `Always` only if `hi` is `Always`.
TEST(KeyConditionBounds, BoundsHoldOnRandomFilters)
{
    tryRegisterFunctions();
    auto context = Context::createCopy(getContext().context);
    std::mt19937 rng(42);
    const DataTypes key_types{std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    constexpr size_t rows_per_mark = 4;
    constexpr size_t num_marks = 8;

    for (size_t iteration = 0; iteration < 300; ++iteration)
    {
        std::vector<TableRow> rows(rows_per_mark * num_marks);
        for (auto & row : rows)
            for (auto & value : row)
                value = std::uniform_int_distribution<Int64>(0, 4)(rng);
        std::ranges::sort(rows, [](const TableRow & l, const TableRow & r) { return std::tie(l[0], l[1]) < std::tie(r[0], r[1]); });

        ActionsDAG dag;
        ActionsDAG::NodeRawConstPtrs inputs;
        for (const char * name : {"a", "b", "c"})
            inputs.push_back(&dag.addInput(name, std::make_shared<DataTypeInt64>()));
        const Filter filter = randomFilter(dag, inputs, context, rng, 3);
        dag.getOutputs().push_back(filter.node);

        ActionsDAG key_dag;
        key_dag.getOutputs().push_back(&key_dag.addInput("a", key_types[0]));
        key_dag.getOutputs().push_back(&key_dag.addInput("b", key_types[1]));
        KeyCondition condition(
            ActionsDAGWithInversionPushDown(filter.node, context, /*boolean_context=*/ true), context, Names{"a", "b"},
            std::make_shared<ExpressionActions>(std::move(key_dag)));

        /// Marks [begin, end) run from the first row of `begin` to the first row of `end`, or to the last row.
        auto truth = [&](size_t begin, size_t end)
        {
            const TableRow & left = rows[begin * rows_per_mark];
            const TableRow & right = rows[std::min(end * rows_per_mark, rows.size() - 1)];
            std::vector<FieldRef> left_keys{Field(left[0]), Field(left[1])};
            std::vector<FieldRef> right_keys{Field(right[0]), Field(right[1])};
            return condition.checkBoundsInRange(2, left_keys.data(), right_keys.data(), key_types);
        };

        for (size_t begin = 0; begin < num_marks; ++begin)
        {
            for (size_t end = begin + 1; end <= num_marks; ++end)
            {
                SCOPED_TRACE(fmt::format("condition {}, marks [{}, {})", condition.toString(), begin, end));
                const Truth range_truth = truth(begin, end);
                const auto matches = std::count_if(rows.begin() + begin * rows_per_mark, rows.begin() + end * rows_per_mark, filter.evaluate);
                const auto total = static_cast<std::ptrdiff_t>((end - begin) * rows_per_mark);
                EXPECT_TRUE(matches == 0 || range_truth.is_true.level() != Side::Never);
                EXPECT_TRUE(matches == total || range_truth.is_true.level() != Side::Always);
                EXPECT_TRUE(matches == total || range_truth.is_false.level() != Side::Never);
                EXPECT_TRUE(matches == 0 || range_truth.is_false.level() != Side::Always);
                EXPECT_NE(range_truth.is_null.level(), Side::Always);

                for (size_t sub_begin = begin; sub_begin < end; ++sub_begin)
                {
                    for (size_t sub_end = sub_begin + 1; sub_end <= end; ++sub_end)
                    {
                        const Truth sub_truth = truth(sub_begin, sub_end);
                        const std::array<std::pair<Side, Side>, 3> sides{
                            std::pair{sub_truth.is_true, range_truth.is_true},
                            std::pair{sub_truth.is_false, range_truth.is_false},
                            std::pair{sub_truth.is_null, range_truth.is_null}};
                        for (const auto & [sub, side] : sides)
                        {
                            EXPECT_TRUE(sub.level() != Side::Never || side.mayBeNever());
                            EXPECT_TRUE(sub.level() != Side::Always || side.mayBeAlways());
                        }
                    }
                }
            }
        }
    }
}
