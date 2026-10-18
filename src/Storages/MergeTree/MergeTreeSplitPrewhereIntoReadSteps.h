#pragma once

#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/ExpressionActions.h>

#include <map>

namespace DB
{

class ColumnsDescription;
struct IndexReadTask;
using IndexReadTasks = std::map<String, IndexReadTask>;

/// Appends the conditions combined with AND into `atoms`, descending into nested AND nodes.
/// The order of the original conditions is preserved: `and(A, and(B, C))` yields `A, B, C`.
/// ActionsDAG::extractConjunctionAtoms is not reused here because it walks with a stack and thus
/// reverses sibling order, while the step boundaries of `tryBuildPrewhereSteps` and the evaluation order promised for a
/// user written PREWHERE both depend on the original order.
void flattenConjunction(const ActionsDAG::Node * node, ActionsDAG::NodeRawConstPtrs & atoms);

/// The conditions combined with AND into the PREWHERE, evaluated in steps by `tryBuildPrewhereSteps`. Unset if the
/// PREWHERE is not split into steps.
std::optional<ActionsDAG::NodeRawConstPtrs> getSplitPrewhereConditions(const PrewhereInfo & prewhere_info);

/// `true_conditions` marks the conditions of `getSplitPrewhereConditions` that are true on every row the steps read. The
/// steps skip them. Empty means none. If every condition is true, there are no steps.
bool tryBuildPrewhereSteps(
    PrewhereInfoPtr prewhere_info,
    const ExpressionActionsSettings & actions_settings,
    PrewhereExprInfo & prewhere,
    bool force_short_circuit_execution,
    const ColumnsDescription * columns = nullptr,
    bool read_ahead_columns = false,
    const IndexReadTasks * index_read_tasks = nullptr,
    const std::vector<bool> & true_conditions = {});

}
