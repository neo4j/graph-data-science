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

import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node.
 *
 * This impl uses another approach where all the triangles can be calculated
 * using set intersection methods of the graph itself.
 *
 *  https://epubs.siam.org/doi/pdf/10.1137/1.9781611973198.1
 *  http://www.cse.cuhk.edu.hk/~jcheng/papers/triangle_kdd11.pdf
 *  https://i11www.iti.kit.edu/extra/publications/sw-fclt-05_t.pdf
 *  http://www.math.cmu.edu/~ctsourak/tsourICDM08.pdf
 *
 * @author mknblch
 */
public class IntersectingTriangleCount extends LegacyAlgorithm<IntersectingTriangleCount> {

    private Graph graph;
    private ExecutorService executorService;
    private final int concurrency;
    private final long nodeCount;
    private final AllocationTracker tracker;
    private final LongAdder triangleCount;
    private final AtomicLong queue;
    private final AtomicLong visitedNodes;
    private PagedAtomicIntegerArray triangles;
    private double averageClusteringCoefficient;

    public IntersectingTriangleCount(Graph graph, ExecutorService executorService, int concurrency, AllocationTracker tracker) {
        this.graph = graph;
        this.tracker = tracker;
        this.executorService = executorService;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        visitedNodes = new AtomicLong();
        triangles = PagedAtomicIntegerArray.newArray(nodeCount, tracker);
        triangleCount = new LongAdder();
        queue = new AtomicLong();
    }

    public long getTriangleCount() {
        return triangleCount.longValue();
    }

    public double getAverageCoefficient() {
        return averageClusteringCoefficient;
    }

    public PagedAtomicIntegerArray getTriangles() {
        return triangles;
    }

    public HugeDoubleArray getCoefficients() {
        final HugeDoubleArray array = HugeDoubleArray.newArray(nodeCount, tracker);
        final double[] adder = new double[]{0.0};
        for (int i = 0; i < nodeCount; i++) {
            final double c = calculateCoefficient(triangles.get(i), graph.degree(i, Direction.OUTGOING));
            array.set(i, c);
            adder[0] += (c);
        }
        averageClusteringCoefficient = adder[0] / nodeCount;
        return array;
    }

    public final Stream<Result> resultStream() {
        return IntStream.range(0, Math.toIntExact(nodeCount))
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        calculateCoefficient(triangles.get(i), graph.degree(i, Direction.OUTGOING))));
    }

    @Override
    public final IntersectingTriangleCount me() {
        return this;
    }

    @Override
    public void release() {
        executorService = null;
        graph = null;
        triangles = null;
    }

    @Override
    public Void compute() {
        visitedNodes.set(0);
        queue.set(0);
        triangleCount.reset();
        averageClusteringCoefficient = 0.0;
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(concurrency, () -> new IntersectTask(graph));
        // run
        ParallelUtil.run(tasks, executorService);
        return null;
    }

    private class IntersectTask implements Runnable, IntersectionConsumer {

        private RelationshipIntersect intersect;

        IntersectTask(Graph graph) {
            intersect = graph.intersection();
        }

        @Override
        public void run() {
            long node;
            while ((node = queue.getAndIncrement()) < nodeCount && running()) {
                intersect.intersectAll(node, this);
                getProgressLogger().logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
        }

        @Override
        public void accept(final long nodeA, final long nodeB, final long nodeC) {
            // only use this triangle where the id's are in order, not the other 5
            if  (nodeA < nodeB) { //  && nodeB < nodeC
                triangles.add((int) nodeA, 1);
                triangles.add((int) nodeB, 1);
                triangles.add((int) nodeC, 1);
                triangleCount.increment();
            }
        }
    }

    private double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return ((double) (triangles << 1)) / (degree * (degree - 1));
    }

    /**
     * result type
     */
    public static class Result {

        public final long nodeId;
        public final long triangles;
        public final double coefficient;

        public Result(long nodeId, long triangles, double coefficient) {
            this.nodeId = nodeId;
            this.triangles = triangles;
            this.coefficient = coefficient;
        }
        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", triangles=" + triangles +
                    ", coefficient=" + coefficient +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Result result = (Result) o;
            return nodeId == result.nodeId &&
                    triangles == result.triangles &&
                    Double.compare(result.coefficient, coefficient) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, triangles, coefficient);
        }
    }
}
