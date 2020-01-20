/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.traverse;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjLongConsumer;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

public final class Traverse extends Algorithm<Traverse, Traverse> {

    public static final Aggregator DEFAULT_AGGREGATOR = (s, t, w) -> .0;

    private final int nodeCount;
    private final Direction direction;
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
        Direction direction,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        ObjLongConsumer<LongArrayDeque> nodeFunc,
        ObjDoubleConsumer<DoubleArrayDeque> weightFunc
    ) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.direction = direction;
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
        Direction direction,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction
    ) {
        return new Traverse(
            graph,
            direction,
            startNodeId,
            exitPredicate,
            aggregatorFunction,
            LongArrayDeque::addFirst,
            DoubleArrayDeque::addFirst
        );
    }

    public static Traverse bfs(
        Graph graph,
        Direction direction,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction
    ) {
        return new Traverse(
            graph,
            direction,
            startNodeId,
            exitPredicate,
            aggregatorFunction,
            LongArrayDeque::addLast,
            DoubleArrayDeque::addLast
        );
    }

    @Override
    public Traverse compute() {
        long sourceNode = graph.toMappedNodeId(startNodeId);
        final LongHashSet result = new LongHashSet(nodeCount);
        nodes.clear();
        sources.clear();
        visited.clear();
        nodeFunc.accept(nodes, sourceNode);
        nodeFunc.accept(sources, sourceNode);
        weightFunc.accept(weights, .0);
        visited.set(sourceNode);

        loop:
        while (!nodes.isEmpty() && running()) {
            final long source = sources.removeFirst();
            final long node = nodes.removeFirst();
            final double weight = weights.removeFirst();
            switch (exitPredicate.test(source, node, weight)) {
                case BREAK:
                    result.add(graph.toOriginalNodeId(node));
                    break loop;
                case CONTINUE:
                    continue loop;
                case FOLLOW:
                    result.add(graph.toOriginalNodeId(node));
                    break;
            }

            graph.forEachRelationship(
                node,
                direction,
                longToIntConsumer((s, t) -> {
                    // remove from the visited nodes to allow revisiting in case the node is accessible via more than one path.
                    double aggregatedWeight = aggregatorFunction.apply(s, t, weight);
                    final ExitPredicate.Result test = exitPredicate.test(s, t, aggregatedWeight);
                    if (test == ExitPredicate.Result.FOLLOW && visited.get(t)) {
                        visited.clear(t);
                    }

                    if (!visited.get(t)) {
                        visited.set(t);

                        nodeFunc.accept(sources, s);
                        nodeFunc.accept(nodes, t);
                        weightFunc.accept(weights, aggregatedWeight);
                    }
                    return running();
                })
            );
        }

        this.resultNodes = result.toArray();
        return me();
    }

    public long[] resultNodes() {
        return resultNodes;
    }

    @Override
    public Traverse me() {
        return this;
    }

    @Override
    public void release() {
        nodes = null;
        weights = null;
        visited = null;
    }

    public interface ExitPredicate {

        enum Result {
            /**
             * add current node to the result set and visit all neighbors
             */
            FOLLOW,
            /**
             * add current node to the result set and terminate traversal
             */
            BREAK,
            /**
             * does not add node to the result set, does not follow its neighbors,
             * just continue with next element on the stack
             */
            CONTINUE
        }

        /**
         * called once for each accepted node during traversal
         *
         * @param sourceNode     the source node
         * @param currentNode    the current node
         * @param weightAtSource the total weight that has been collected by the Aggregator during the traversal
         * @return a result
         */
        Result test(long sourceNode, long currentNode, double weightAtSource);
    }


    public interface Aggregator {

        /**
         * aggregate weight between source and current node
         *
         * @param sourceNode     source node
         * @param currentNode    the current node
         * @param weightAtSource the weight that has been aggregated for the currentNode so far
         * @return new weight (e.g. weightAtSource + 1.)
         */
        double apply(long sourceNode, long currentNode, double weightAtSource);
    }
}
