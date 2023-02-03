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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArrayStack;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public class DFS extends Algorithm<HugeLongArray> {

    private final Graph graph;
    private final long startNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private final long nodeCount;

    private final long maxDepth;

    public DFS(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        long maxDepth,
        ProgressTracker progressTracker
    ) {

        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.startNodeId = startNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.maxDepth = maxDepth;
    }

    @Override
    public HugeLongArray compute() {
        progressTracker.beginSubTask();
        var result = HugeLongArray.newArray(nodeCount);
        var nodes = HugeLongArrayStack.newStack(nodeCount);
        var sources = HugeLongArrayStack.newStack(nodeCount);
        var weights = HugeDoubleArrayStack.newStack(nodeCount);
        
        var visited = new BitSet(nodeCount);
        nodes.push(startNodeId);
        sources.push(startNodeId);
        weights.push(.0);
        visited.set(startNodeId);

        long resultIndex = 0L;
        while (!nodes.isEmpty() && terminationFlag.running()) {
            final long source = sources.pop();
            final long node = nodes.pop();
            final double weight = weights.pop();

            result.set(resultIndex++, node);

            var exitPredicateResult = exitPredicate.test(source, node, weight);

            if (exitPredicateResult == ExitPredicate.Result.BREAK) {
                break;
            }


            // For disconnected graphs or early termination, this will not reach 100
            progressTracker.logProgress(graph.degree(node));

            //If there is a maximum depth, and node is already at it, do not waste time with its neighborhood
            if (maxDepth == DfsBaseConfig.NO_MAX_DEPTH || weight < maxDepth) {
                graph.forEachRelationship(
                    node,
                    (s, t) -> {
                        if (!visited.get(t)) {
                                visited.set(t);
                                sources.push(s);
                                nodes.push(t);
                                weights.push(aggregatorFunction.apply(s, t, weight));
                        }
                        return terminationFlag.running();
                    }
                );
            }
        }

        progressTracker.endSubTask();
        return result.copyOf(resultIndex);
    }

}
