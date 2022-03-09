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
import com.carrotsearch.hppc.DoubleStack;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongStack;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import static org.neo4j.gds.Converters.longToIntConsumer;

public class DFS extends Algorithm<long[]> {

    private final Graph graph;
    private final long startNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private final int nodeCount;

    public DFS(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        ProgressTracker progressTracker
    ) {

        super(progressTracker);
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.startNodeId = startNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
    }

    @Override
    public long[] compute() {
        progressTracker.beginSubTask();
        var result = new LongArrayList(nodeCount);
        var inResult = new BitSet(nodeCount);

        var nodes = new LongStack(nodeCount);
        var sources = new LongStack(nodeCount);
        var weights = new DoubleStack(nodeCount);
        var visited = new BitSet(nodeCount);
        nodes.push(startNodeId);
        sources.push(startNodeId);
        weights.push(.0);
        visited.set(startNodeId);

        while (!nodes.isEmpty() && running()) {
            final long source = sources.pop();
            final long node = nodes.pop();
            final double weight = weights.pop();

            var exitPredicateResult = exitPredicate.test(source, node, weight);
            if (exitPredicateResult == ExitPredicate.Result.CONTINUE) {
                continue;
            } else {
                if (!inResult.getAndSet(node)) {
                    result.add(node);
                }
                if (exitPredicateResult == ExitPredicate.Result.BREAK) {
                    break;
                }
            }

            // For disconnected graphs or early termination, this will not reach 100
            progressTracker.logProgress(graph.degree(node));

            graph.forEachRelationship(
                node,
                longToIntConsumer((s, t) -> {
                    if (!visited.get(t)) {
                        visited.set(t);
                        sources.push(s);
                        nodes.push(t);
                        weights.push(aggregatorFunction.apply(s, t, weight));
                    }
                    return running();
                })
            );
        }

        progressTracker.endSubTask();
        return result.toArray();
    }

    @Override
    public void release() {

    }
}
