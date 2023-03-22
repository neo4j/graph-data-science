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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Arrays;

final class ComputeNodeToCommunityWeights implements Runnable {

    private final Graph graph;
    private final byte k;
    private final double defaultWeight;
    private final LocalSearch.WeightTransformer weightTransformer;
    private final HugeByteArray candidateSolution;
    private final HugeAtomicDoubleArray nodeToCommunityWeights;
    private final Partition partition;
    private final ProgressTracker progressTracker;

    ComputeNodeToCommunityWeights(
        Graph graph,
        byte k,
        double defaultWeight,
        LocalSearch.WeightTransformer weightTransformer,
        HugeByteArray candidateSolution,
        HugeAtomicDoubleArray nodeToCommunityWeights,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.k = k;
        this.defaultWeight = defaultWeight;
        this.weightTransformer = weightTransformer;
        this.candidateSolution = candidateSolution;
        this.nodeToCommunityWeights = nodeToCommunityWeights;
        this.partition = partition;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        // We keep a local tab to minimize atomic accesses.
        var outgoingImprovementCosts = new double[k];

        partition.consume(nodeId -> {
            Arrays.fill(outgoingImprovementCosts, 0.0D);

            graph.forEachRelationship(
                nodeId,
                defaultWeight,
                (sourceNodeId, targetNodeId, weight) -> {
                    // Loops don't affect the cut cost.
                    if (sourceNodeId == targetNodeId) return true;

                    double transformedWeight = weightTransformer.accept(weight);

                    outgoingImprovementCosts[candidateSolution.get(targetNodeId)] += transformedWeight;

                    // This accounts for the `nodeToCommunityWeight` for the incoming relationship
                    // `sourceNodeId -> targetNodeId` from `targetNodeId`'s point of view.
                    // TODO: We could avoid these cache-unfriendly accesses of the outgoing relationships if we had
                    //  a way to traverse incoming relationships (pull-based traversal).
                    nodeToCommunityWeights.getAndAdd(
                        targetNodeId * k + candidateSolution.get(sourceNodeId),
                        transformedWeight
                    );

                    return true;
                }
            );

            for (int i = 0; i < k; i++) {
                nodeToCommunityWeights.getAndAdd(nodeId * k + i, outgoingImprovementCosts[i]);
            }
        });

        progressTracker.logProgress(partition.nodeCount());
    }
}
