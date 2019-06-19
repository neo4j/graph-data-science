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

import com.carrotsearch.hppc.IntStack;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

import static org.neo4j.graphalgo.core.utils.Converters.longToIntConsumer;

/**
 *
 * ForkJoin approach to make triangle count concurrent
 *
 * @author mknblch
 */
public class TriangleCountForkJoin extends TriangleCountBase<AtomicDoubleArray, TriangleCountForkJoin> {

    private final ForkJoinPool pool;
    private final int sequentialThreshold;
    private final AtomicDoubleArray coefficients;
    private long triangleCount = -1;
    private double averageClusteringCoefficient = 0.0d;

    public TriangleCountForkJoin(Graph graph, ForkJoinPool pool, int sequentialThreshold) {
        super(graph);
        this.pool = pool;
        this.sequentialThreshold = sequentialThreshold;
        coefficients = new AtomicDoubleArray(nodeCount);
    }

    /**
     * return coefficient of a (inner)nodeId
     * @param node
     * @return
     */
    @Override
    double coefficient(final int node) {
        return coefficients.get(node);
    }

    /**
     * get number of triangle is the graph
     * @return
     */
    @Override
    public long getTriangleCount() {
        return triangleCount;
    }

    @Override
    void runCompute() {
        ForkJoinTask<Long> countTask =
                graph.getType().equals(HugeGraph.TYPE) ?
                        new HugeTask(graph, 0, nodeCount) :
                        new TriangleTask(0, nodeCount);
        triangleCount = pool.invoke(countTask);
        CoefficientTask coefficientTask = new CoefficientTask(
                Direction.OUTGOING,
                0,
                nodeCount);
        averageClusteringCoefficient = pool.invoke(coefficientTask);
    }

    /**
     * get average clustering coefficient
     * @return
     */
    @Override
    public double getAverageClusteringCoefficient() {
        return averageClusteringCoefficient / nodeCount;
    }

    /**
     * get nodeId to clustering coefficient mapping
     * @return
     */
    @Override
    public AtomicDoubleArray getClusteringCoefficients() {
        return coefficients;
    }

    /**
     * task for counting triangles at each node
     */
    private class TriangleTask extends RecursiveTask<Long> {

        private final RelationshipIterator localRelationshipIterator = graph.concurrentCopy();
        private final RelationshipIterator localRelationshipPredicate = graph.concurrentCopy();

        private final int start;
        private final int end;

        private TriangleTask(int start, int end) {
            this.start = start;
            this.end = end;
        }

        @Override
        protected Long compute() {
            final int l = end - start;
            if (l > sequentialThreshold && running()) {
                final int pivot = start + l / 2;
                final TriangleTask left = new TriangleTask(start, pivot);
                final TriangleTask right = new TriangleTask(pivot, end);
                left.fork();
                return right.compute() + left.join();
            } else {
                return execute(start, end);
            }
        }

        private long execute(final int start, final int end) {
            final long triangles[] = {0L};
            final IntStack nodes = new IntStack();
            final TerminationFlag flag = getTerminationFlag();
            final int[] head = {-1};
            for (head[0] = start; head[0] < end; head[0]++) {
                localRelationshipIterator.forEachRelationship(head[0], D, longToIntConsumer((s, t) -> {
                    if (t > s) {
                        nodes.push(t);
                    }
                    return flag.running();
                }));
                while (!nodes.isEmpty()) {
                    final int node = nodes.pop();
                    localRelationshipIterator.forEachRelationship(node, D, longToIntConsumer((s, t) -> {
                        if (t > s && localRelationshipPredicate.exists(t, head[0], D)) {
                            exportTriangle(head[0], s, t);
                            triangles[0]++;
                        }
                        return flag.running();
                    }));
                }
                nodeVisited();
            }
            return triangles[0];
        }
    }

    /**
     * task for calculating node clustering coefficients
     */
    private class CoefficientTask extends RecursiveTask<Double> {

        private final Direction direction;
        private final int start;
        private final int end;

        private CoefficientTask(Direction direction, int start, int end) {
            this.direction = direction;
            this.start = start;
            this.end = end;
        }

        @Override
        protected Double compute() {
            final int l = end - start;
            if (l > sequentialThreshold && running()) {
                final int pivot = start + l / 2;
                final CoefficientTask left = new CoefficientTask(direction, start, pivot);
                final CoefficientTask right = new CoefficientTask(direction, pivot, end);
                left.fork();
                return right.compute() + left.join();
            } else {
                return execute(start, end);
            }
        }

        private double execute(final int start, final int end) {
            double averageClusteringCoefficient = 0.0;
            for (int i = start; i < end; i++) {
                final double c = calculateCoefficient(i, direction);
                averageClusteringCoefficient += c;
                coefficients.set(i, c);
            }
            return averageClusteringCoefficient;
        }
    }

    private class HugeTask extends RecursiveTask<Long> implements IntersectionConsumer {

        private Graph graph;
        private RelationshipIntersect hg;
        private final int start;
        private final int end;

        private long count;

        HugeTask(Graph graph, int start, int end) {
            this.graph = graph;
            this.start = start;
            this.end = end;
            hg = graph.intersection();
        }

        @Override
        protected Long compute() {
            final int l = end - start;
            if (l > sequentialThreshold && running()) {
                final int pivot = start + l / 2;
                final HugeTask left = new HugeTask(graph, start, pivot);
                final HugeTask right = new HugeTask(graph, pivot, end);
                left.fork();
                return right.compute() + left.join();
            } else {
                return execute(start, end);
            }
        }

        private long execute(int start, int end) {
            for (int node = start; node < end && running(); node++) {
                hg.intersectAll(node, this);
                nodeVisited();
            }
            return count;
        }

        @Override
        public void accept(final long nodeA, final long nodeB, final long nodeC) {
            ++count;
            exportTriangle((int) nodeA, (int) nodeB, (int) nodeC);
        }
    }
}
