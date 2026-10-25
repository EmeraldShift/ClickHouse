#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorPromoter.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>

#include <base/types.h>

#include <map>
#include <vector>

namespace DB
{

using LocalPartsByPartition = std::map<String, std::vector<MergeTreePartInfo>>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

/// The range of the blocks of `part` in its partition. A patch part has the range of the versions of its lightweight
/// updates in its original partition. An update takes a block number of the partition and adds no rows.
MergeTreePartInfo getBlockRange(const IMergeTreeDataPart & part);

/// The block ranges of `parts` by partition, sorted by `min_block`. A data part also has the block of the last mutation or
/// update that it holds. No other part may cover that block, and the part has lost its rows as of a cursor that stops
/// before the block.
LocalPartsByPartition getLocalParts(const DataPartsVector & parts);

/// The highest block number that `cursor` reaches through `parts` of one partition, sorted by `min_block`, without
/// crossing a block that `promoter` reports as not fetched or committing.
Int64 promoteBlockNumber(Int64 cursor, const std::vector<MergeTreePartInfo> & parts, const MergeTreeCursorPromoter & promoter);

/// Advance the subscription's `safe_block_number` to the highest block reachable without crossing a
/// not-fetched/committing block. Returns true if the round produced new work for the readers.
bool enrichSubscription(
    MergeTreeBoundsSubscription & subscription,
    const LocalPartsByPartition & local_parts,
    const CursorPromotersMap & promoters);

}
