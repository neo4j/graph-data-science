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
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjLongConsumer;

import static org.neo4j.gds.Converters.longToIntConsumer;

public final class Traverse extends Algorithm<Traverse> {

    public static final Aggregator DEFAULT_AGGREGATOR = (s, t, w) -> .0;

    private final int nodeCount;
    private final long startNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private final ObjLongConsumer<LongArrayDeque> nodeFunc;
    private final ObjDoubleConsumer<DoubleArrayDeque> weightFunc;
    private final Graph graph;
    private LongArrayDeque nodes;
    private final LongArrayDeque sources;
    private DoubleArrayDeque weights;
    private BitSet visited;

    private long[] resultNodes;

    private Traverse(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        ObjLongConsumer<LongArrayDeque> nodeFunc,
        ObjDoubleConsumer<DoubleArrayDeque> weightFunc,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.startNodeId = startNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.nodeFunc = nodeFunc;
        this.weightFunc = weightFunc;
        this.nodes = new LongArrayDeque(nodeCount);
        this.sources = new LongArrayDeque(nodeCount);
        this.weights = new DoubleArrayDeque(nodeCount);
        this.visited = new BitSet(nodeCount);
    }

    public static Traverse dfs(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        ProgressTracker progressTracker
    ) {
        return new Traverse(
            graph,
            startNodeId,
            exitPredicate,
            aggregatorFunction,
            LongArrayDeque::addFirst,
            DoubleArrayDeque::addFirst,
            progressTracker
        );
    }

    public static Traverse bfs(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        ProgressTracker progressTracker
    ) {
        return new Traverse(
            graph,
            startNodeId,
            exitPredicate,
            aggregatorFunction,
            LongArrayDeque::addLast,
            DoubleArrayDeque::addLast,
            progressTracker
        );
    }

    @Override
    public Traverse compute() {
        progressTracker.beginSubTask();
        LongArrayList result = new LongArrayList(nodeCount);
        BitSet inResult = new BitSet(nodeCount);
        nodes.clear();
        sources.clear();
        visited.clear();
        nodeFunc.accept(nodes, startNodeId);
        nodeFunc.accept(sources, startNodeId);
        weightFunc.accept(weights, .0);
        visited.set(startNodeId);

        loop:
        while (!nodes.isEmpty() && running()) {
            final long source = sources.removeFirst();
            final long node = nodes.removeFirst();
            final double weight = weights.removeFirst();
            switch (exitPredicate.test(source, node, weight)) {
                case BREAK:
                    if(!inResult.getAndSet(node)) {
                        result.add(node);
                    }
                    break loop;
                case CONTINUE:
                    continue loop;
                case FOLLOW:
                    if (!inResult.getAndSet(node)) {
                        result.add(node);
                    }
                    break;
            }
            // For disconnected graphs or early termination, this will not reach 100
            progressTracker.logProgress(graph.degree(node));

            graph.forEachRelationship(
                node,
                longToIntConsumer((s, t) -> {
                    if (!visited.get(t)) {
                        visited.set(t);
                        nodeFunc.accept(sources, s);
                        nodeFunc.accept(nodes, t);
                        weightFunc.accept(weights, aggregatorFunction.apply(s, t, weight));
                    }
                    return running();
                })
            );
        }

        this.resultNodes = result.toArray();
        progressTracker.endSubTask();
        return this;
    }

    public long[] resultNodes() {
        return resultNodes;
    }

    @Override
    public void release() {
        nodes = null;
        weights = null;
        visited = null;
    }


}
