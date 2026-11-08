#pragma once

#include <unordered_map>

#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Pruning partitions in verbatim way using KeyCondition
class PartitionPruner
{
public:
    PartitionPruner(
        const StorageMetadataPtr & metadata,
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        bool skip_analysis = false,
        bool require_ready_sets = false);

    /// True if the part cannot hold a row the predicate accepts. An empty part never can, whatever
    /// its partition is, which is the answer a read wants.
    bool canBePruned(const IMergeTreeDataPart & part) const;

    /// True if the predicate accepts no value of the part's *partition*. Unlike `canBePruned`, this
    /// says nothing about the part itself: an empty part is answered for by its partition value like
    /// any other. This is what scoping a mutation needs - a partition an empty part sits in can hold
    /// rows on another replica, and pruning it by that part would let them escape the mutation.
    bool canPartitionBePruned(const IMergeTreeDataPart & part) const;

    /// The truth of the predicate on the rows of the part's partition. The part is pruned if `is_true` is `Never`, and the
    /// predicate accepts every row of the part if `is_true` is `Always`.
    Truth checkPartition(const IMergeTreeDataPart & part) const;

    bool isUseless() const { return useless; }

    const KeyCondition & getKeyCondition() const { return partition_condition; }

private:
    /// Cache already analyzed partitions.
    mutable std::unordered_map<String, Truth> partition_truth_map;

    /// partition_key is adjusted here (with substitution from modulo to moduloLegacy).
    KeyDescription partition_key;

    KeyCondition partition_condition;

    bool useless = false;
};

}
