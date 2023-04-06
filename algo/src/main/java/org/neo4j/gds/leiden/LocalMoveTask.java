/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.leiden;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import java.util.concurrent.atomic.AtomicLong;

public class LocalMoveTask implements Runnable {

    static MemoryEstimation estimation() {
        return MemoryEstimations.builder()
            .perNode("neighbor communities", HugeLongArray::memoryEstimation)
            .perNode("neighbor weights", HugeDoubleArray::memoryEstimation)
            .add("local queue", HugeLongArrayQueue.memoryEstimation())
            .build();
    }

    private static final long LOCAL_QUEUE_BOUND = 1000;
    private final Graph graph;
    private final AtomicLong globalQueueIndex;
    private final HugeLongArray encounteredCommunities;
    private final HugeDoubleArray encounteredCommunitiesWeights;
    private final HugeDoubleArray nodeVolumes;

    private final HugeLongArray globalQueue;
    private final AtomicLong globalQueueSize;
    private final HugeAtomicBitSet nodeInQueue;
    private final HugeLongArrayQueue localQueue;
    private final HugeLongArray currentCommunities;
    private final HugeAtomicDoubleArray communityVolumes;
    private long encounteredCommunityCounter = 0;

    private LocalMoveTaskPhase phase;

    private final double gamma;

     long swaps;

    public LocalMoveTask(
        Graph graph,
        HugeLongArray currentCommunities,
        HugeAtomicDoubleArray communityVolumes,
        HugeDoubleArray nodeVolumes,
        HugeLongArray globalQueue,
        AtomicLong globalQueueIndex,
        AtomicLong globalQueueSize,
        HugeAtomicBitSet nodeInQueue,
        double gamma
    ) {
        this.graph = graph;
        this.globalQueue = globalQueue;
        this.globalQueueIndex = globalQueueIndex;
        this.globalQueueSize = globalQueueSize;
        this.encounteredCommunities = HugeLongArray.newArray(graph.nodeCount());
        this.encounteredCommunitiesWeights = HugeDoubleArray.newArray(graph.nodeCount());
        encounteredCommunitiesWeights.setAll(c -> -1L);

        this.nodeVolumes = nodeVolumes;
        this.communityVolumes = communityVolumes;
        this.currentCommunities = currentCommunities;
        this.nodeInQueue = nodeInQueue;
        this.localQueue = HugeLongArrayQueue.newQueue(graph.nodeCount());
        this.gamma = gamma;
        this.phase = LocalMoveTaskPhase.RUN;

    }

    @Override
    public void run() {
        if (phase == LocalMoveTaskPhase.RUN) {
            long offset;
            while ((offset = globalQueueIndex.getAndAdd(64)) < globalQueueSize.get()) {
                var chunkSize = Math.min(offset + 64, globalQueueSize.get());
                for (long idx = offset; idx < chunkSize; idx++) {
                    long nodeId = globalQueue.get(idx);
                    processNode(nodeId);
                }
            }
            //do some local processing if the localQueue is small enough
            while (localQueue.size() > 0 && localQueue.size() < LOCAL_QUEUE_BOUND) {
                long nodeId = localQueue.remove();
                processNode(nodeId);
            }
            phase = LocalMoveTaskPhase.SYNC;
        } else {
            sync();
            this.phase = LocalMoveTaskPhase.RUN;
        }
    }

    private void sync() {
        long currentIndex = globalQueueSize.getAndAdd(localQueue.size());

        while (!localQueue.isEmpty()) {
            globalQueue.set(currentIndex++, localQueue.remove());
        }
    }
    private void moveNodeToNewCommunity(
        long nodeId,
        long newCommunityId,

        double currentNodeVolume
    ) {
        long oldCommunityId = currentCommunities.get(nodeId);
        currentCommunities.set(nodeId, newCommunityId);
        communityVolumes.getAndAdd(newCommunityId, currentNodeVolume);
        //do a atomic update to never go into negatives etc
        communityVolumes.update(oldCommunityId, (oldValue) -> {
            var diff = oldValue - currentNodeVolume;
            return Math.max(diff, 0.0);
        });
        swaps++;

    }

    private void findCommunityRelationshipWeights(long nodeId) {
        encounteredCommunityCounter = 0;
        graph.forEachRelationship(nodeId, 1.0, (s, t, relationshipWeight) -> {
            long tCommunity = currentCommunities.get(t);
            if (encounteredCommunitiesWeights.get(tCommunity) < 0) {
                encounteredCommunities.set(encounteredCommunityCounter, tCommunity);
                encounteredCommunityCounter++;
                encounteredCommunitiesWeights.set(tCommunity, relationshipWeight);
            } else {
                encounteredCommunitiesWeights.addTo(tCommunity, relationshipWeight);
            }

            return true;
        });
    }

    // all neighbours of the node that do not belong to the node’s new community
    // "and that are not yet in the queue are added to the rear of the queue"
    //we check the global queue for existence and add it in the local queue for syncing.
    private void visitNeighboursAfterMove(long nodeId, long movedToCommunityId) {
        graph.forEachRelationship(nodeId, (s, t) -> {
            long tCommunity = currentCommunities.get(t);
            boolean shouldAddInQueue = !nodeInQueue.get(t) && tCommunity != movedToCommunityId;
            if (shouldAddInQueue && !nodeInQueue.getAndSet(t)) {
                localQueue.add(t);
            }
            return true;
        });
    }


    enum LocalMoveTaskPhase {
        RUN, SYNC
    }

    private void tryToMoveNode(
        long nodeId,
        long currentNodeCommunityId,
        double currentNodeVolume,
        long bestCommunityId
    ) {
        boolean shouldChangeCommunity = bestCommunityId != currentNodeCommunityId;
        if (shouldChangeCommunity) {
            moveNodeToNewCommunity(
                nodeId,
                bestCommunityId,
                currentNodeVolume
            );
            visitNeighboursAfterMove(nodeId, bestCommunityId);
        }
    }

    private long findBestCommunity(
        double currentBestGain,
        double currentNodeVolume,
        long bestCommunityId,
        long communityId
    ) {

        for (long i = 0; i < encounteredCommunityCounter; ++i) {
            long candidateCommunityId = encounteredCommunities.get(i);

            double candidateCommunityRelationshipsWeight = encounteredCommunitiesWeights.get(candidateCommunityId);
            encounteredCommunitiesWeights.set(candidateCommunityId, -1);
            if (candidateCommunityId == communityId) {
                continue;
            }
            // Compute the modularity gain for the candidate community
            double modularityGain =
                candidateCommunityRelationshipsWeight - currentNodeVolume * communityVolumes.get(candidateCommunityId) * gamma;

            boolean improves = modularityGain > currentBestGain // gradually better modularity gain
                               // tie-breaking case; consider only positive modularity gains
                               || (modularityGain > 0
                                   // when the current gain is equal to the best gain
                                   && Double.compare(modularityGain, currentBestGain) == 0
                                   // consider it as improvement only if the candidate community ID is lower than the best community ID
                                   // similarly to the Louvain implementation
                                   && candidateCommunityId < bestCommunityId);

            if (improves) {
                bestCommunityId = candidateCommunityId;
                currentBestGain = modularityGain;
            }
        }
        return bestCommunityId;
    }

    private void processNode(long nodeId) {
        nodeInQueue.clear(nodeId);
        long currentNodeCommunityId = currentCommunities.get(nodeId);
        double currentNodeVolume = nodeVolumes.get(nodeId);

        // Remove the current node volume from its community volume

        double modifiedCommunityVolume = communityVolumes.get(currentNodeCommunityId) - currentNodeVolume;

        findCommunityRelationshipWeights(nodeId);

        // Compute the "modularity" for the current node and current community
        double currentBestGain =
            Math.max(0, encounteredCommunitiesWeights.get(currentNodeCommunityId)) -
            currentNodeVolume * modifiedCommunityVolume * gamma;

        long bestCommunityId = findBestCommunity(
            currentBestGain,
            currentNodeVolume,
            currentNodeCommunityId,
            currentNodeCommunityId
        );

        tryToMoveNode(
            nodeId,
            currentNodeCommunityId,
            currentNodeVolume,
            bestCommunityId
        );

    }
}
