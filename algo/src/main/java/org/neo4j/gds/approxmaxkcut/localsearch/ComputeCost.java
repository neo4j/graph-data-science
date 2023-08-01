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
package org.neo4j.gds.approxmaxkcut.localsearch;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.AtomicDouble;
import org.neo4j.gds.collections.ha.HugeByteArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

final class ComputeCost implements Runnable {

    private final Graph graph;
    private final double defaultWeight;
    private final LocalSearch.WeightTransformer weightTransformer;
    private final HugeByteArray candidateSolution;
    private final AtomicDouble cost;
    private final Partition partition;
    private final ProgressTracker progressTracker;

    ComputeCost(
        Graph graph,
        double defaultWeight,
        LocalSearch.WeightTransformer weightTransformer,
        HugeByteArray candidateSolution,
        AtomicDouble cost,
        Partition partition,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.defaultWeight = defaultWeight;
        this.weightTransformer = weightTransformer;
        this.candidateSolution = candidateSolution;
        this.cost = cost;
        this.partition = partition;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        // We keep a local tab to minimize atomic accesses.
        var localCost = new MutableDouble(0.0);

        partition.consume(nodeId ->
            graph.forEachRelationship(
                nodeId,
                defaultWeight,
                (sourceNodeId, targetNodeId, weight) -> {
                    if (candidateSolution.get(sourceNodeId) != candidateSolution.get(targetNodeId)) {
                        localCost.add(weightTransformer.accept(weight));
                    }
                    return true;
                }
            ));
        cost.getAndAdd(localCost.doubleValue());

        progressTracker.logProgress(partition.nodeCount());
    }
}
