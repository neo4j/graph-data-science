/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.triangle;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Balanced triads algorithm.
 *
 * The algorithm calculates the number of balanced and unbalanced triads
 * of a node. A triangle or triad is balanced if the product of the weights
 * of their relations is positive, or unbalanced if negative:
 *
 *
 * positive:                negative:
 *
 *        (b)                   (b)
 *      +/  \+                -/   \-
 *    (a)---(c)              (a)---(c)
 *        -                      -
 *
 * see: https://en.wikipedia.org/wiki/Balance_theory
 *
 * The algorithm should run on a complete graph where each node
 * is connected to each of its possible neighbors by either a
 * relation with positive or negative weight.
 *
 * @author mknblch
 */
public class BalancedTriads extends Algorithm<BalancedTriads> {

    public interface BalancedPredicate {

        boolean isBalanced(double w1, double w2, double w3);
    }

    private Graph graph;
    private ExecutorService executorService;
    private final PagedAtomicIntegerArray balancedTriangles;
    private final PagedAtomicIntegerArray unbalancedTriangles;
    private final int concurrency;
    private final long nodeCount;
    private final LongAdder balancedTriangleCount;
    private final LongAdder unbalancedTriangleCount;
    private final AtomicLong queue;
    private final AtomicLong visitedNodes;
    private BalancedPredicate balancedPredicate = (w1, w2, w3) -> (w1 * w2 * w3) >= .0;

    public BalancedTriads(Graph graph, ExecutorService executorService, int concurrency, AllocationTracker tracker) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        visitedNodes = new AtomicLong();
        balancedTriangles = PagedAtomicIntegerArray.newArray(nodeCount, tracker);
        unbalancedTriangles = PagedAtomicIntegerArray.newArray(nodeCount, tracker);
        balancedTriangleCount = new LongAdder();
        unbalancedTriangleCount = new LongAdder();
        queue = new AtomicLong();
    }

    @Override
    public final BalancedTriads me() {
        return this;
    }

    /**
     * release inner data structs
     */
    @Override
    public void release() {
        executorService = null;
        graph = null;
        unbalancedTriangles.release();
        balancedTriangles.release();
        balancedTriangleCount.reset();
        unbalancedTriangleCount.reset();
    }

    /**
     * compute number of balanced and unbalanced triangles
     * @return
     */
    public BalancedTriads compute() {
        visitedNodes.set(0);
        queue.set(0);
        balancedTriangleCount.reset();
        unbalancedTriangleCount.reset();
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(concurrency, () -> new HugeTask(graph));
        // run
        ParallelUtil.run(tasks, executorService);
        return this;
    }

    /**
     * get result stream of original node id to number of balanced
     * and unbalanced triangles
     * @return
     */
    public Stream<Result> stream() {
        return IntStream.range(0, Math.toIntExact(nodeCount))
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        balancedTriangles.get(i),
                        unbalancedTriangles.get(i)));
    }

    /**
     * @return array of nodeId to number of balanced triangles mapping
     */
    public PagedAtomicIntegerArray getBalancedTriangles() {
        return balancedTriangles;
    }

    /**
     * @return array of nodeId to number of UNbalanced triangles mapping
     */
    public PagedAtomicIntegerArray getUnbalancedTriangles() {
        return unbalancedTriangles;
    }

    /**
     * get number of balanced triads
     * @return
     */
    public long getBalancedTriangleCount() {
        return balancedTriangleCount.longValue();
    }

    /**
     * get number of unbalanced triads
     * @return
     */
    public long getUnbalancedTriangleCount() {
        return unbalancedTriangleCount.longValue();
    }

    /**
     * result triple for original nodeId, number of balanced and unbalanced triangles
     */
    public static class Result {

        public final long nodeId;
        public final long balanced;
        public final long unbalanced;

        public Result(long nodeId, long balanced, long unbalanced) {
            this.nodeId = nodeId;
            this.balanced = balanced;
            this.unbalanced = unbalanced;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", balanced=" + balanced +
                    ", unbalanced=" + unbalanced +
                    '}';
        }
    }

    private class HugeTask implements Runnable, IntersectionConsumer {

        private RelationshipIntersect hg;

        HugeTask(Graph graph) {
            hg = graph.intersection();
        }

        @Override
        public void run() {
            long node;
            while ((node = queue.getAndIncrement()) < nodeCount && running()) {
                hg.intersectAll(node, this);
                getProgressLogger().logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
        }

        @Override
        public void accept(final long a, final long b, final long c) {
            if (balancedPredicate.isBalanced(graph.relationshipProperty(a, b, 1.0D), graph.relationshipProperty(a, c, 1.0D), graph.relationshipProperty(b, c, 1.0D))) {
                balancedTriangles.add(a, 1);
                balancedTriangles.add(b, 1);
                balancedTriangles.add(c, 1);
                balancedTriangleCount.increment();
            } else {
                unbalancedTriangles.add(a, 1);
                unbalancedTriangles.add(b, 1);
                unbalancedTriangles.add(c, 1);
                unbalancedTriangleCount.increment();
            }
        }
    }
}
