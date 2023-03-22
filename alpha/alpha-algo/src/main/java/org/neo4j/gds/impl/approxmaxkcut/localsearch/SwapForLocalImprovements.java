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
package org.neo4j.gds.impl.approxmaxkcut.localsearch;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicByteArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutConfig;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

final class SwapForLocalImprovements implements Runnable {

    private final Graph graph;
    private final ApproxMaxKCutConfig config;
    private final ApproxMaxKCut.Comparator comparator;
    private final HugeByteArray candidateSolution;
    private final AtomicLongArray cardinalities;
    private final HugeAtomicDoubleArray nodeToCommunityWeights;
    private final HugeAtomicByteArray swapStatus;
    private final AtomicBoolean change;
    private final Partition partition;
    private final ProgressTracker progressTracker;
    private final byte k;

    SwapForLocalImprovements(
        Graph graph,
        ApproxMaxKCutConfig config,
        ApproxMaxKCut.Comparator comparator,
        HugeByteArray candidateSolution,
        AtomicLongArray cardinalities,
        HugeAtomicDoubleArray nodeToCommunityWeights,
        HugeAtomicByteArray swapStatus,
        AtomicBoolean change,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.config = config;
        this.comparator = comparator;
        this.candidateSolution = candidateSolution;
        this.cardinalities = cardinalities;
        this.nodeToCommunityWeights = nodeToCommunityWeights;
        this.swapStatus = swapStatus;
        this.change = change;
        this.k = config.k();
        this.partition = partition;
        this.progressTracker = progressTracker;
    }

    /*
     * Used to improve readability of swapping.
     */
    static final class NodeSwapStatus {
        // No thread has tried to swap the node yet, and no incoming neighbor has tried to mark it.
        static final byte UNTOUCHED = 0;
        // The node has been swapped to another community, or a thread is currently attempting to swap it.
        static final byte SWAPPING = 1;
        // The node has had one of its incoming neighbors swapped (or at least attempted) so the improvement costs may be invalid.
        static final byte NEIGHBOR = 2;

        private NodeSwapStatus() {}
    }

    @Override
    public void run() {
        var failedSwap = new MutableBoolean();
        var localChange = new MutableBoolean(false);

        partition.consume(nodeId -> {
            byte currentCommunity = candidateSolution.get(nodeId);
            byte bestCommunity = bestCommunity(nodeId, currentCommunity);

            if (bestCommunity == currentCommunity) return;

            // Check if we're allowed to move this node from its current community.
            long updatedCardinality = cardinalities.getAndUpdate(
                currentCommunity,
                currentCount -> {
                    if (currentCount > config.minCommunitySizes().get(currentCommunity)) {
                        return currentCount - 1;
                    }
                    return currentCount;
                }
            );
            if (updatedCardinality == config.minCommunitySizes().get(currentCommunity)) {
                return;
            }

            localChange.setTrue();

            // If no incoming neighbors has marked us as NEIGHBOR, we can attempt to swap the current node.
            if (!swapStatus.compareAndSet(nodeId, NodeSwapStatus.UNTOUCHED, NodeSwapStatus.SWAPPING)) {
                cardinalities.getAndIncrement(currentCommunity);
                return;
            }

            failedSwap.setFalse();

            // We check all outgoing neighbors to see that they are not SWAPPING. And if they aren't we mark them
            // NEIGHBOR so that they don't try to swap later ProgressTracker       progressTracker // Default value: progressTracker.
            graph.forEachRelationship(
                nodeId,
                0.0,
                (sourceNodeId, targetNodeId, weight) -> {
                    // Loops should not stop us.
                    if (targetNodeId == sourceNodeId) return true;

                    // We try to mark the outgoing neighbor as NEIGHBOR to make sure it doesn't swap as well.
                    if (!swapStatus.compareAndSet(
                        targetNodeId,
                        NodeSwapStatus.UNTOUCHED,
                        NodeSwapStatus.NEIGHBOR
                    )) {
                        if (swapStatus.get(targetNodeId) != NodeSwapStatus.NEIGHBOR) {
                            // This outgoing neighbor must be SWAPPING and so we can no longer rely on the computed
                            // improvement cost of our current node being correct and therefore we abort.
                            failedSwap.setTrue();
                            return false;
                        }
                    }
                    return true;
                }
            );

            if (failedSwap.isTrue()) {
                // Since we didn't complete the swap for this node we can unmark it as SWAPPING and thus not
                // stopping incoming neighbor nodes of swapping because of this node's status.
                swapStatus.set(nodeId, NodeSwapStatus.NEIGHBOR);
                cardinalities.getAndIncrement(currentCommunity);
                return;
            }

            candidateSolution.set(nodeId, bestCommunity);
            cardinalities.getAndIncrement(bestCommunity);
        });

        if (localChange.getValue()) change.set(true);

        progressTracker.logProgress(partition.nodeCount());
    }

    private byte bestCommunity(long nodeId, byte currentCommunity) {
        final long NODE_OFFSET = nodeId * k;
        byte bestCommunity = currentCommunity;
        double bestCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + currentCommunity);

        for (byte i = 0; i < k; i++) {
            var nodeToCommunityWeight = nodeToCommunityWeights.get(NODE_OFFSET + i);
            if (comparator.compare(bestCommunityWeight, nodeToCommunityWeight)) {
                bestCommunity = i;
                bestCommunityWeight = nodeToCommunityWeight;
            }
        }

        return bestCommunity;
    }
}
