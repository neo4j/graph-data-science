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

import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.container.Buckets;
import org.neo4j.graphdb.Direction;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.heavyweight.Converters.longToIntConsumer;

/**
 * parallel non-negative single source shortest path algorithm
 * <p>
 * Delta-Stepping is a parallel non-negative single source shortest paths (NSSSP) algorithm
 * to calculate the length of the shortest paths from a starting node to all other
 * nodes in the graph. It can be tweaked using the delta-parameter which controls
 * the grade of concurrency.<br>
 * <p>
 * More information in:<br>
 * <p>
 * <a href="https://arxiv.org/pdf/1604.02113v1.pdf">https://arxiv.org/pdf/1604.02113v1.pdf</a><br>
 * <a href="https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf">https://ae.cs.uni-frankfurt.de/pdf/diss_uli.pdf</a><br>
 * <a href="http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf">http://www.cc.gatech.edu/~bader/papers/ShortestPaths-ALENEX2007.pdf</a><br>
 * <a href="http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf">http://www.dis.uniroma1.it/challenge9/papers/madduri.pdf</a>
 *
 * @author mknblch
 */
public class ShortestPathDeltaStepping extends LegacyAlgorithm<ShortestPathDeltaStepping> {

    // distance array
    private AtomicIntegerArray distance;
    // bucket impl
    private Buckets buckets;
    private Graph graph;
    // collections of runnables containing either heavy edge relax-operations or light edge relax-ops.
    private Collection<Runnable> light, heavy;
    // list of futures of light and heavy edge relax-operations
    private Collection<Future<?>> futures;

    // delta parameter
    private final double delta;
    private final int nodeCount;
    // scaled delta
    private int iDelta;

    private ExecutorService executorService;

    // multiplier used to scale an double to int
    private double multiplier = 100_000D; // double type is intended
    private Direction direction;

    public ShortestPathDeltaStepping(Graph graph, double delta, Direction direction) {
        this.graph = graph;
        this.delta = delta;
        this.iDelta = (int) (multiplier * delta);
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.direction = direction;
        distance = new AtomicIntegerArray(nodeCount);
        buckets = new Buckets(nodeCount);
        heavy = new ArrayDeque<>(1024);
        light = new ArrayDeque<>(1024);
        futures = new ArrayDeque<>(128);
    }

    /**
     * Set Executor-service to enable concurrent evaluation.
     *
     * @param executorService the executor service or null do disable concurrent eval.
     * @return itself for method chaining
     */
    public ShortestPathDeltaStepping withExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * set the multiplier used to scale up double weights to integers
     *
     * @param multiplier the multiplier
     * @return itself for method chaining
     */
    public ShortestPathDeltaStepping withMultiplier(int multiplier) {
        if (multiplier < 1) {
            throw new IllegalArgumentException("multiplier must be >= 1");
        }
        this.multiplier = multiplier;
        this.iDelta = (int) (multiplier * delta);
        return this;
    }

    /**
     * compute the shortest path
     *
     * @param startNode UNmapped (original) neo4j nodeId as starting point
     * @return itself for method chaining
     */
    public ShortestPathDeltaStepping compute(long startNode) {

        // reset
        for (int i = 0; i < nodeCount; i++) {
            distance.set(i, Integer.MAX_VALUE);
        }
        buckets.reset();

        // basically assign start node to bucket 0
        relax(Math.toIntExact(graph.toMappedNodeId(startNode)), 0);

        // as long as the bucket contains any value
        while (!buckets.isEmpty() && running()) {
            // reset temporary arrays
            light.clear();
            heavy.clear();

            // get next bucket index
            final int phase = buckets.nextNonEmptyBucket();

            // for each node in bucket
            buckets.forEachInBucket(phase, node -> {
                // relax each outgoing light edge
                RelationshipWithPropertyConsumer relationshipConsumer = longToIntConsumer((sourceNodeId, targetNodeId, cost) -> {
                    final int iCost = (int) (cost * multiplier + distance.get(sourceNodeId));
                    if (cost <= delta) { // determine if light or heavy edge
                        light.add(() -> relax(targetNodeId, iCost));
                    } else {
                        heavy.add(() -> relax(targetNodeId, iCost));
                    }
                    return true;
                });

                graph.forEachRelationship(node, direction, 0.0D, relationshipConsumer);
                return true;
            });
            ParallelUtil.run(light, executorService, futures);
            ParallelUtil.run(heavy, executorService, futures);
        }
        return this;
    }

    /**
     * get downscaled sum of distance
     *
     * @param nodeId the mapped node-id
     * @return the overall distance from source to nodeId
     */
    private double get(int nodeId) {
        int distance = this.distance.get(nodeId);
        return distance == Integer.MAX_VALUE ? Double.POSITIVE_INFINITY : distance / multiplier;
    }

    /**
     * compare and set. tries to store the new calculated costs
     * as long as no other thread has already written a value
     * smaller then cost. if another thread writes a value bigger
     * then cost the loop tries again otherwise the function
     * terminates.
     *
     * @param nodeId
     * @param cost
     */
    private void cas(int nodeId, int cost) {
        boolean stored = false;
        while (!stored) {
            int oldC = distance.get(nodeId);
            if (cost < oldC) {
                stored = distance.compareAndSet(nodeId, oldC, cost);
            } else {
                break;
            }
        }
    }

    /**
     * relax() sets the summed cost in {@link ShortestPathDeltaStepping#distance}
     * if they are smaller then the current cost - like dijkstra. If so it also
     * calculates the next bucket index for the node and assigns it.
     *
     * @param nodeId node id
     * @param cost   the summed cost
     */
    private void relax(int nodeId, int cost) {
        if (cost >= distance.get(nodeId)) {
            return;
        }
        int bucketIndex = (cost / iDelta); // calculate bucket index
        buckets.set(nodeId, bucketIndex);
        cas(nodeId, cost);
    }

    /**
     * scale down integer representation to double[]
     *
     * @return mapped-id to costSum array
     */
    public double[] getShortestPaths() {
        double[] d = new double[nodeCount];
        for (int i = nodeCount - 1; i >= 0; i--) {
            d[i] = get(i);
        }
        return d;
    }

    /**
     * stream the results
     *
     * @return Stream of results containing neo4j-NodeId and Sum of Costs of the shortest path
     */
    public Stream<DeltaSteppingResult> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(node ->
                        new DeltaSteppingResult(graph.toOriginalNodeId(node), get(node)));
    }

    @Override
    public ShortestPathDeltaStepping me() {
        return this;
    }

    @Override
    public void release() {
        distance = null;
        buckets = null;
        graph = null;
        light = null;
        heavy = null;
        futures = null;
    }

    /**
     * Basic result DTO
     */
    public static class DeltaSteppingResult {

        /**
         * the neo4j node id
         */
        public final long nodeId;
        /**
         * minimum distance from startNode to nodeId
         */
        public final double distance;

        public DeltaSteppingResult(long nodeId, double distance) {
            this.nodeId = nodeId;
            this.distance = distance;
        }

        @Override
        public String toString() {
            return "DeltaSteppingResult{" +
                   "nodeId=" + nodeId +
                   ", distance=" + distance +
                   '}';
        }
    }
}
