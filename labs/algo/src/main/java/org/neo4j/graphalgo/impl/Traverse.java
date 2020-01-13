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
package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.impl.traverse.TraverseConfig;
import org.neo4j.graphdb.Direction;

import java.util.List;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

public class Traverse extends Algorithm<Traverse, Traverse> {

    public enum TraverseAlgo {
        BFS,
        DFS
    }

    public static final Aggregator DEFAULT_AGGREGATOR = (s, t, w) -> .0;

    private final int nodeCount;
    private final TraverseAlgo traverseAlgo;
    private final Direction direction;
    private final long startNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private Graph graph;
    private LongArrayDeque nodes;
    private LongArrayDeque sources;
    private DoubleArrayDeque weights;
    private BitSet visited;

    private long[] resultNodes;

    public Traverse(Graph graph, TraverseAlgo traverseAlgo, Direction direction, long startNodeId, ExitPredicate exitPredicate, Aggregator aggregatorFunction) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.traverseAlgo = traverseAlgo;
        this.direction = direction;
        this.startNodeId = startNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.nodes = new LongArrayDeque(nodeCount);
        this.sources = new LongArrayDeque(nodeCount);
        this.weights = new DoubleArrayDeque(nodeCount);
        this.visited = new BitSet(nodeCount);
    }

    @Override
    public Traverse compute() {
        if (traverseAlgo == TraverseAlgo.BFS) {
            computeBfs(
                startNodeId,
                direction,
                exitPredicate,
                aggregatorFunction);
        } else {
            computeDfs(
                startNodeId,
                direction,
                exitPredicate,
                aggregatorFunction);
        }

        return me();
    }

    public long[] resultNodes() {
        return resultNodes;
    }

    /**
     * start BFS without aggregator
     *
     * @param sourceId      source node id
     * @param direction     traversal direction
     * @param exitCondition
     * @return
     */
    private void computeBfs(long sourceId, Direction direction, ExitPredicate exitCondition) {
        computeBfs(
            graph.toMappedNodeId(sourceId),
            direction,
            exitCondition,
            DEFAULT_AGGREGATOR
        );
    }

    /**
     * start DSF without aggregator
     *
     * @param sourceId      source node id
     * @param direction     traversal direction
     * @param exitCondition
     * @return
     */
    private void computeDfs(long sourceId, Direction direction, ExitPredicate exitCondition) {
        computeDfs(
            graph.toMappedNodeId(sourceId),
            direction,
            exitCondition,
            (s, t, w) -> .0
        );
    }

    /**
     * start BFS using an aggregator function
     *
     * @param sourceId      source node id
     * @param direction     traversal direction
     * @param exitCondition
     * @param aggregator
     * @return
     */
    private void computeBfs(long sourceId, Direction direction, ExitPredicate exitCondition, Aggregator aggregator) {
        traverse(
            graph.toMappedNodeId(sourceId),
            direction,
            exitCondition,
            aggregator,
            LongArrayDeque::addLast,
            DoubleArrayDeque::addLast
        );
    }

    /**
     * start DFS using an aggregator function
     *
     * @param sourceId      source node id
     * @param direction     traversal direction
     * @param exitCondition
     * @param aggregator
     * @return
     */
    private void computeDfs(long sourceId, Direction direction, ExitPredicate exitCondition, Aggregator aggregator) {
        traverse(
            graph.toMappedNodeId(sourceId),
            direction,
            exitCondition,
            aggregator,
            LongArrayDeque::addFirst,
            DoubleArrayDeque::addFirst
        );
    }

    /**
     * traverse along the path
     *
     * @param sourceNode    source node (mapped id)
     * @param direction     the traversal direction
     * @param exitCondition exit condition
     * @param agg           weight accumulator function
     * @param nodeFunc      node accessor function (either ::addLast or ::addFirst to switch between fifo and lifo behaviour)
     * @param weightFunc    weight accessor function (either ::addLast or ::addFirst to switch between fifo and lifo behaviour)
     * @return a list of nodes that have been visited
     */
    private void traverse(
        long sourceNode,
        Direction direction,
        ExitPredicate exitCondition,
        Aggregator agg,
        ObjLongConsumer<LongArrayDeque> nodeFunc,
        ObjDoubleConsumer<DoubleArrayDeque> weightFunc
    ) {
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
            switch (exitCondition.test(source, node, weight)) {
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
                    double aggregatedWeight = agg.apply(s, t, weight);
                    final ExitPredicate.Result test = exitCondition.test(s, t, aggregatedWeight);
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
