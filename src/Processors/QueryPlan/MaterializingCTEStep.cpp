#include <Processors/QueryPlan/MaterializingCTEStep.h>

#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

namespace
{

constexpr ITransformingStep::Traits getMaterializingCTETraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

}

MaterializingCTEStep::MaterializingCTEStep(
    SharedHeader input_header_,
    MaterializedCTEPtr materialized_cte_
)
    : ITransformingStep(std::move(input_header_), std::make_shared<const Block>(Block{}), getMaterializingCTETraits())
    , materialized_cte(std::move(materialized_cte_))
{
}

void MaterializingCTEStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    pipeline.addMaterializingCTETransform(getOutputHeader(), materialized_cte);
}

void MaterializingCTEStep::describeActions([[maybe_unused]] JSONBuilder::JSONMap & map) const
{
}

void MaterializingCTEStep::describeActions([[maybe_unused]] FormatSettings & settings) const
{
}

MaterializingCTEsStep::MaterializingCTEsStep(SharedHeaders input_headers_)
    : IQueryPlanStep()
{
    input_headers = std::move(input_headers_);
    output_header = input_headers.front();
}

QueryPipelineBuilderPtr MaterializingCTEsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializingCTEsStep cannot be created with no inputs");

    auto main_pipeline = std::move(pipelines.front());
    if (pipelines.size() == 1)
        return main_pipeline;

    pipelines.erase(pipelines.begin());

    QueryPipelineBuilder delayed_pipeline;
    if (pipelines.size() > 1)
    {
        QueryPipelineProcessorsCollector collector(delayed_pipeline, this);
        delayed_pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines));
        processors = collector.detachProcessors();
    }
    else
        delayed_pipeline = std::move(*pipelines.front());

    QueryPipelineProcessorsCollector collector(*main_pipeline, this);
    main_pipeline->addPipelineBefore(std::move(delayed_pipeline));
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());

    return main_pipeline;
}

DelayedMaterializingCTEsStep::DelayedMaterializingCTEsStep(
    SharedHeader input_header,
    std::vector<MaterializedCTEPtr> ctes_
)
    : ctes(std::move(ctes_))
{
    input_headers = {input_header};
    output_header = std::move(input_header);
}

QueryPipelineBuilderPtr DelayedMaterializingCTEsStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot build pipeline in DelayedMaterializingCTEs. This step should be optimized out.");
}

std::vector<std::unique_ptr<QueryPlan>> DelayedMaterializingCTEsStep::makePlansForCTEs(
    DelayedMaterializingCTEsStep && step,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    std::vector<std::unique_ptr<QueryPlan>> plans;

    /// Correctness of CTE visibility comes from the future barrier on
    /// each MaterializedCTE rather than from the plan-tree placement of
    /// MaterializingCTEsStep. Any consumer that sees an unbuilt CTE —
    /// including eager optimizer-time consumers fired from the same
    /// `plan.optimize()` call (KeyCondition → buildOrderedSetInplace,
    /// DelayedCreatingSetsStep::makePlansForSets, evaluateScalarSubqueryIfNeeded)
    /// — must find the CTE's storage populated. Synchronously run
    /// each claimed CTE's build here, at the top of plan optimization,
    /// so the build completes before any sub-pipeline fires.
    ///
    /// For EXPLAIN and for deserialized plans without a captured
    /// query_context, keep the plan-return shape (wire the CTE plan into
    /// the outer tree for later execution) so EXPLAIN PLAN remains
    /// side-effect-free.
    const bool can_build_inplace = !optimization_settings.is_explain
        && optimization_settings.query_context != nullptr;

    for (auto & materialized_cte : step.ctes)
    {
        if (!can_build_inplace)
        {
            /// EXPLAIN / remote-deserialized path: keep the legacy
            /// plan-return shape — optimize here, hand the plan off to
            /// the outer tree for side-effect-free rendering.
            ///
            /// Dedup: a CTE may appear in several DelayedMaterializing
            /// CTEsSteps (e.g. in force_materialize_cte sub-planner
            /// scenarios). After the first emission through this branch,
            /// `materialized_cte->plan` has been moved out and is null.
            /// Subsequent calls skip, which replaces the old
            /// `is_materialization_planned.exchange(true)` guard.
            if (!materialized_cte->plan)
                continue;
            materialized_cte->plan->optimize(optimization_settings);
            plans.emplace_back(std::move(materialized_cte->plan));
            continue;
        }

        /// buildInplace uses the builder installed on the MaterializedCTE's
        /// future (MaterializedCTE::MaterializedCTE): optimize + build +
        /// CompletedPipelineExecutor::execute, returning the populated
        /// storage. On failure the future transitions to State::Failed and
        /// the exception propagates back to this caller.
        materialized_cte->future->buildInplace(optimization_settings.query_context);
    }
    return plans;
}

}
