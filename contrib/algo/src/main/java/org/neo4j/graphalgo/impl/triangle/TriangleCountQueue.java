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
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node.
 *
 * Experimental implementation with a queue based parallelization approach
 *
 * @author mknblch
 */
public class TriangleCountQueue extends Algorithm<TriangleCountQueue> implements TriangleCountAlgorithm {

    private final Graph graph;
    private ExecutorService executorService;
    private final int nodeCount;
    private final AtomicIntegerArray triangles;
    private final AtomicInteger visitedNodes;
    private final int concurrency;
    private final Direction direction;
    private final AtomicInteger queue;
    private final LongAdder triangleCount;
    private double averageClusteringCoefficient;

    public TriangleCountQueue(Graph graph, ExecutorService executorService, int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        this.concurrency = concurrency;
        triangleCount = new LongAdder();
        direction = Direction.OUTGOING;
        queue = new AtomicInteger();
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        triangles = new AtomicIntegerArray(nodeCount);
        visitedNodes = new AtomicInteger();
    }

    @Override
    public long getTriangleCount() {
        return triangleCount.longValue();
    }

    @Override
    public double getAverageCoefficient() {
        return averageClusteringCoefficient;
    }

    @Override
    public AtomicIntegerArray getTriangles() {
        return triangles;
    }

    @Override
    public double[] getCoefficients() {
        final double[] array = new double[nodeCount];
        final double[] adder = new double[]{0.0};
        for (int i = 0; i < nodeCount; i++) {
            final double c = TriangleCountAlgorithm.calculateCoefficient(triangles.get(i), graph.degree(i, direction));
            array[i] = c;
            adder[0] += c;
        }
        averageClusteringCoefficient = adder[0] / nodeCount;
        return array;
    }

    @Override
    public Stream<Result> resultStream() {
        return IntStream.range(0, Math.toIntExact(nodeCount))
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        TriangleCountAlgorithm.calculateCoefficient(triangles.get(i), graph.degree(i, direction))));
    }


    @Override
    public TriangleCountQueue me() {
        return this;
    }

    @Override
    public TriangleCountQueue release() {
        executorService = null;
        return this;
    }

    @Override
    public TriangleCountQueue compute() {
        queue.set(0);
        triangleCount.reset();
        averageClusteringCoefficient = 0.0;
        // create tasks
        final Collection<? extends Runnable> tasks = ParallelUtil.tasks(concurrency, Task::new);
        // run
        ParallelUtil.run(tasks, executorService);
        return this;
    }

    private class Task implements Runnable {

        @Override
        public void run() {
            final IntStack nodes = new IntStack();
            final TerminationFlag flag = getTerminationFlag();
            final int[] head = new int[1];
            while ((head[0] = queue.getAndIncrement()) < nodeCount) {
                graph.forEachRelationship(head[0], direction, (s, t, r) -> {
                    if (t > s) {
                        nodes.push(t);
                    }
                    return flag.running();
                });
                while (!nodes.isEmpty()) {
                    final int node = nodes.pop();
                    graph.forEachRelationship(node, direction, (s, t, r) -> {
                        if (t > s && graph.exists(t, head[0], direction)) {
                            triangles.incrementAndGet(head[0]);
                            triangles.incrementAndGet(s);
                            triangles.incrementAndGet(t);
                            triangleCount.increment();

                        }
                        return flag.running();
                    });
                }
                getProgressLogger().logProgress(visitedNodes.incrementAndGet(), nodeCount);
            }
        }
    }

}
