#include <Storages/MergeTree/PartitionPruner.h>
#include <Common/logger_useful.h>

namespace DB
{

PartitionPruner::PartitionPruner(
    const StorageMetadataPtr & metadata,
    const ActionsDAGWithInversionPushDown & filter_dag,
    ContextPtr context,
    bool skip_analysis,
    bool require_ready_sets)
    : partition_key(MergeTreePartition::adjustPartitionKey(metadata, context))
    , partition_condition(
          filter_dag,
          context,
          partition_key.column_names,
          partition_key.expression,
          true /* single_point */,
          skip_analysis,
          require_ready_sets)
    , useless(partition_condition.alwaysUnknownOrTrue())
{
}

bool PartitionPruner::canBePruned(const IMergeTreeDataPart & part) const
{
    if (part.isEmpty())
        return true;

    return canPartitionBePruned(part);
}

bool PartitionPruner::canPartitionBePruned(const IMergeTreeDataPart & part) const
{
    return checkPartition(part).is_true.level() == Side::Never;
}

Truth PartitionPruner::checkPartition(const IMergeTreeDataPart & part) const
{
    const auto & partition_id = part.info.getPartitionId();
    if (auto it = partition_truth_map.find(partition_id); it != partition_truth_map.end())
        return it->second;

    const auto & partition_value = part.partition.value;
    std::vector<FieldRef> index_value(partition_value.begin(), partition_value.end());
    for (auto & field : index_value)
    {
        // NULL_LAST
        if (field.isNull())
            field = POSITIVE_INFINITY;
    }

    const auto truth = partition_condition.checkBoundsInRange(
        partition_value.size(), index_value.data(), index_value.data(), partition_key.data_types);
    partition_truth_map.emplace(partition_id, truth);

    if (truth.is_true.level() == Side::Never)
    {
        LOG_TRACE(getLogger("PartitionPruner"), "Partition {} gets pruned",
            part.partition.serializeToString(partition_key.sample_block));
    }

    return truth;
}

}
