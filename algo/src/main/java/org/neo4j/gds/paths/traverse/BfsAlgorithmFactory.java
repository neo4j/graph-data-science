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
package org.neo4j.gds.paths.traverse;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.List;
import java.util.stream.Collectors;

public class BfsAlgorithmFactory<CONFIG extends BfsBaseConfig> extends GraphAlgorithmFactory<BFS, CONFIG> {

    @Override
    public BFS build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
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
            progressTracker,
            configuration.maxDepth()
        );
    }

    @Override
    public String taskName() {
        return "BFS";
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(BFS.class);

        builder.perNode("visited ", HugeAtomicBitSet::memoryEstimation) //global variables
            .perNode("traversedNodes", HugeLongArray::memoryEstimation)
            .perNode("weights", HugeDoubleArray::memoryEstimation)
            .perNode("minimumChunk", HugeAtomicLongArray::memoryEstimation);

        //per thread
        builder.rangePerGraphDimension("localNodes", (dimensions, concurrency) -> {
            // lower-bound: each node is in exactly one localNode array
            var lowerBound = MemoryUsage.sizeOfLongArrayList(dimensions.nodeCount() + dimensions.nodeCount() / 64);

            //In the upper bound, we can consider two scenarios:
            //  -each node except the starting will be added by every thread exactly once
            //  -traversing each relationship creates an entry in any localNodes array of one of the threads
            //We can take the minimum of these as a more accurate upper bound.
            //Nonetheless, either of those scenarios is unlikely to happen because all nodes in all threads
            //need to be added at the exact same step to force such memory usage in an extremely convoluted way
            var maximumTotalSizeOfAggregatedLocalNodes = Math.min(
                dimensions.relCountUpperBound(),
                concurrency * (dimensions.nodeCount() - 1)
            );

            var upperBound = MemoryUsage.sizeOfLongArrayList(maximumTotalSizeOfAggregatedLocalNodes + dimensions.nodeCount() / 64);
            //The  nodeCount()/64 refers to the  chunk separator in localNodes
            return MemoryRange.of(lowerBound, Math.max(lowerBound, upperBound));
        }).perGraphDimension("chunks", (dimensions, concurrency) ->
            MemoryRange.of(dimensions.nodeCount() / 64)
        );

        builder.perNode("resultNodes", HugeLongArray::memoryEstimation);


        return builder.build();
    }
}
