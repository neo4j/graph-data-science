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
package org.neo4j.graphalgo.impl.betweenness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.container.MultiQueue;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implements Betweenness Centrality for unweighted graphs.
 * <p>
 * The algorithm is based on Brandes definition but with some changes
 * regarding the dependency-accumulation step.
 * <p>
 * taken from: http://cass-mt.pnnl.gov/docs/pubs/georgiatechlbnlpnnlfastbc-mtaap2009.pdf
 *
 * TODO: unstable (deadlocks) & deprecated (only used in tests) ?
 * @author mknblch
 */
public class BetweennessCentralitySuccessorBrandes extends Algorithm<BetweennessCentralitySuccessorBrandes> {

    // the graph
    private Graph graph;
    // atomic double array which supports only atomic-add
    private AtomicDoubleArray centrality;
    // the node count
    private int nodeCount;
    // global executor service
    private ExecutorService executorService;

    private AtomicIntegerArray sigma;
    private AtomicIntegerArray d;
    private double[] delta;
    private AtomicInteger count;
    private int phase;
    private MultiQueue successors;
    private MultiQueue phaseQueue;
    private ArrayList<Future<?>> futures = new ArrayList<>();
    private Direction direction = Direction.OUTGOING;

    /**
     * constructs a parallel centrality solver
     *
     * @param graph           the graph iface
     * @param executorService the executor service
     */
    public BetweennessCentralitySuccessorBrandes(Graph graph, ExecutorService executorService) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.executorService = executorService;
        this.centrality = new AtomicDoubleArray(nodeCount);
        sigma = new AtomicIntegerArray(nodeCount);
        delta = new double[nodeCount];
        d = new AtomicIntegerArray(nodeCount);
        successors = new MultiQueue(executorService, nodeCount);
        phaseQueue = new MultiQueue(executorService, nodeCount);
        count = new AtomicInteger();
    }

    /**
     * compute centrality
     *
     * @return itself for method chaining
     */
    public BetweennessCentralitySuccessorBrandes compute() {
        graph.forEachNode(this::compute);
        if (direction == Direction.BOTH) {
            ParallelUtil.iterateParallel(executorService, nodeCount, Pools.DEFAULT_CONCURRENCY, i -> {
                centrality.set(i, centrality.get(i) / 2.0);
            });
        }
        return this;
    }

    public BetweennessCentralitySuccessorBrandes withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    private boolean compute(long startNode) {

        // This will break for very large graphs
        int startNodeId = (int) startNode;

        // initialization

        final AtomicIntegerArray offsets = phaseQueue.getOffsets();

        ParallelUtil.iterateParallel(executorService, nodeCount, 4, i -> {
            d.set(i, -1);
            sigma.set(i, 0);
            offsets.set(i, 0);
        });

        phaseQueue.addOrCreate(0, startNodeId);
        d.set(startNodeId, 0);
        sigma.set(startNodeId, 1);

        phase = 0;
        count.set(1);

        // computation
        while (count.getAndSet(0) > 0 && running()) {
            futures.clear();
            phaseQueue.forEach(futures, phase, v -> { // in parallel
                successors.clear(v);
                graph.concurrentCopy().forEachRelationship(
                        v,
                        direction,
                        (sourceNodeId, target) -> {
                            // This will break for very large graphs
                            int w = (int) target;
                            int dw = d.get(w);
                            d.compareAndSet(w, -1, phase + 1);
                            if (dw == -1) {
                                phaseQueue.addOrCreate(phase + 1, w);
                                count.incrementAndGet();
                                dw = phase + 1;
                            }
                            if (dw == phase + 1) {
                                sigma.addAndGet(w, sigma.get(v));
                                successors.addOrCreate(v, w);
                            }
                            return true;
                        });
            });
            ParallelUtil.awaitTermination(futures);
            phase++;
        }

        // back propagation + dependency accumulation
        Arrays.fill(delta, 0D);
        while (--phase > 0 && running()) {
            futures.clear();
            phaseQueue.forEach(futures, phase, w -> {
                final double[] dsw = {0.0};
                final double sw = sigma.get(w);
                successors.forEach(
                        w,
                        v -> dsw[0] += (sw / (double) sigma.get(v)) * (1.0 + delta[v]));
                delta[w] = dsw[0];
                centrality.add(w, dsw[0]);
            });
            ParallelUtil.awaitTermination(futures);
        }

        return true;
    }

    /**
     * get the centrality array
     *
     * @return array with centrality
     */
    public AtomicDoubleArray getCentrality() {
        return centrality;
    }

    /**
     * iterate over each result until every node has
     * been visited or the consumer returns false
     *
     * @param consumer the result consumer
     */
    public void forEach(BetweennessCentrality.ResultConsumer consumer) {
        for (int i = nodeCount - 1; i >= 0; i--) {
            if (!consumer.consume(
                    graph.toOriginalNodeId(i),
                    centrality.get(i))) {
                return;
            }
        }
    }

    /**
     * emit the result stream
     *
     * @return stream if Results
     */
    public Stream<BetweennessCentrality.Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId ->
                        new BetweennessCentrality.Result(
                                graph.toOriginalNodeId(nodeId),
                                centrality.get(nodeId)));
    }


    @Override
    public BetweennessCentralitySuccessorBrandes me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        centrality = null;
        executorService = null;
        sigma = null;
        d = null;
        delta = null;
        count = null;
        successors = null;
        phaseQueue = null;
        futures = null;
    }
}
