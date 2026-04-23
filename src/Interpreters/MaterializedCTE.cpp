#include <Interpreters/MaterializedCTE.h>

#include <Common/thread_local_rng.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
namespace Setting
{
extern const SettingsUInt64 interactive_delay;
}

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

MaterializedCTE::MaterializedCTE(const std::string & cte_name_)
    : cte_name(cte_name_)
    , temporary_table_name(fmt::format("_materialized_cte_{}_{}", cte_name, thread_local_rng()))
{}

MaterializedCTE::~MaterializedCTE() noexcept = default;

void MaterializedCTE::buildIfNeeded(const ContextPtr & context)
{
    if (is_built.load(std::memory_order_acquire))
        return;

    /// `resolveMaterializingCTEs` runs after the two tree-optimization
    /// passes, so readers that land here get first crack at claiming the
    /// CTE. A losing exchange means a pass ran out of the expected order,
    /// which we'd rather hear about loudly than paper over.
    if (is_materialization_planned.exchange(true))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Materialized CTE '{}' was already claimed for build",
            cte_name);

    chassert(plan != nullptr);

    plan->optimize(QueryPlanOptimizationSettings(context));

    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

    CompletedPipelineExecutor executor(pipeline);
    if (context->hasQueryContext())
    {
        if (auto cancel_callback = context->getQueryContext()->getInteractiveCancelCallback())
            executor.setCancelCallback(
                std::move(cancel_callback),
                std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
    }
    executor.execute();

    plan.reset();
}

}
