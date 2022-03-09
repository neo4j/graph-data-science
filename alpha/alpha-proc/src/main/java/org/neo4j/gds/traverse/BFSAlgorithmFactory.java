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
package org.neo4j.gds.traverse;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.traverse.Aggregator;
import org.neo4j.gds.paths.traverse.BFS;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.gds.paths.traverse.ExitPredicate;
import org.neo4j.gds.paths.traverse.MaxDepthExitPredicate;
import org.neo4j.gds.paths.traverse.OneHopAggregator;
import org.neo4j.gds.paths.traverse.TargetExitPredicate;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.InputNodeValidator.validateEndNode;
import static org.neo4j.gds.utils.InputNodeValidator.validateStartNode;

class BFSAlgorithmFactory extends GraphAlgorithmFactory<BFS, BfsStreamConfig> {

    @Override
    public BFS build(
        Graph graph, BfsStreamConfig configuration, ProgressTracker progressTracker
    ) {
        validateStartNode(configuration.sourceNode(), graph);
        configuration.targetNodes().forEach(neoId -> validateEndNode(neoId, graph));

        ExitPredicate exitFunction;
        Aggregator aggregatorFunction;
        // target node given; terminate if target is reached
        if (!configuration.targetNodes().isEmpty()) {
            List<Long> mappedTargets = configuration.targetNodes().stream()
                .map(graph::safeToMappedNodeId)
                .collect(Collectors.toList());
            exitFunction = new TargetExitPredicate(mappedTargets);
            aggregatorFunction = Aggregator.NO_AGGREGATION;
            // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (configuration.maxDepth() != -1) {
            exitFunction = new MaxDepthExitPredicate(configuration.maxDepth());
            aggregatorFunction = new OneHopAggregator();
            // do complete BFS until all nodes have been visited
        } else {
            exitFunction = ExitPredicate.FOLLOW;
            aggregatorFunction = Aggregator.NO_AGGREGATION;
        }

        var mappedStartNodeId = graph.toMappedNodeId(configuration.sourceNode());

        return BFS.create(
            graph,
            mappedStartNodeId,
            exitFunction,
            aggregatorFunction,
            configuration.concurrency(),
            progressTracker
        );
    }

    @Override
    public String taskName() {
        return "BFS";
    }

    @Override
    public MemoryEstimation memoryEstimation(BfsStreamConfig configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(BFS.class);

        builder.perNode("visited ", HugeAtomicBitSet::memoryEstimation) //global variables
            .perNode("traversedNodes", HugeLongArray::memoryEstimation)
            .perNode("sources", HugeLongArray::memoryEstimation)
            .perNode("weights", HugeDoubleArray::memoryEstimation)
            .perNode("minimumChunk", HugeAtomicLongArray::memoryEstimation);

        //per thread
        builder.rangePerGraphDimension("localNodes", (dimensions, concurrency) -> {
            // lower-bound: each node is in exactly one localNode array
            var lowerBound = MemoryUsage.sizeOfLongArrayList(dimensions.nodeCount() + dimensions.nodeCount() / 64);
            // This is the worst-case, which we will most likely never hit since the
            // graph needs to be complete to reach all nodes from all threads. Also each node needs to be accessed at the same time by all threads
            var upperBound = MemoryUsage.sizeOfLongArrayList(dimensions.relCountUpperBound() + dimensions.nodeCount() / 64);
            //The  nodeCount()/64 refers to the  chunk separator in localNodes
            return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
        }).rangePerGraphDimension("localWeights", (dimensions, concurrency) -> {
            // lower-bound: each node is in exactly one localNode array
            var lowerBound = MemoryUsage.sizeOfDoubleArrayList(dimensions.nodeCount() + dimensions.nodeCount() / 64);
            // This is the worst-case, which we will most likely never hit since the
            // graph needs to be complete to reach all nodes from all threads. Also each node needs to be accessed at the same time by all threads
            var upperBound = MemoryUsage.sizeOfDoubleArrayList(dimensions.relCountUpperBound() + dimensions.nodeCount() / 64);
            //The  nodeCount()/64 refers to the  chunk separator in localNodes
            return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
        }).perGraphDimension("chunks", (dimensions, concurrency) ->
            MemoryRange.of(dimensions.nodeCount() / 64)
        );

        builder.perNode("resultNodes", MemoryUsage::sizeOfLongArray);


        return builder.build();
    }
}
