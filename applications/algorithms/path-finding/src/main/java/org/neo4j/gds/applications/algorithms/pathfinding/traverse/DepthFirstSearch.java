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
package org.neo4j.gds.applications.algorithms.pathfinding.traverse;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.traverse.Aggregator;
import org.neo4j.gds.paths.traverse.DFS;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.traverse.ExitPredicate;
import org.neo4j.gds.paths.traverse.OneHopAggregator;
import org.neo4j.gds.paths.traverse.TargetExitPredicate;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.stream.Collectors;

public class DepthFirstSearch {
    public HugeLongArray compute(Graph graph, DfsBaseConfig configuration, ProgressTracker progressTracker, TerminationFlag terminationFlag) {
        ExitPredicate exitFunction;
        Aggregator aggregatorFunction;
        // target node given; terminate if target is reached
        if (configuration.hasTargetNodes()) {
            List<Long> mappedTargets = configuration.targetNodes().stream()
                .map(graph::safeToMappedNodeId)
                .collect(Collectors.toList());
            exitFunction = new TargetExitPredicate(mappedTargets);
            aggregatorFunction = Aggregator.NO_AGGREGATION;
            // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (configuration.hasMaxDepth()) {
            exitFunction = ExitPredicate.FOLLOW;
            aggregatorFunction = new OneHopAggregator();
            // do complete DFS until all nodes have been visited
        } else {
            exitFunction = ExitPredicate.FOLLOW;
            aggregatorFunction = Aggregator.NO_AGGREGATION;
        }

        var mappedSourceNodeId = graph.toMappedNodeId(configuration.sourceNode());
        var maxDepth = configuration.maxDepth();
        var dfs = new DFS(
            graph,
            mappedSourceNodeId,
            exitFunction,
            aggregatorFunction,
            maxDepth,
            progressTracker,
            terminationFlag
        );

        return new AlgorithmMachinery().runAlgorithmsAndManageProgressTracker(
            dfs,
            progressTracker,
            true,
            configuration.concurrency()
        );
    }
}
