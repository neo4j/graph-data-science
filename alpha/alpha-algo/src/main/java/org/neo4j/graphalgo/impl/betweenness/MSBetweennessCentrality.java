/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.LongIntScatterMap;
import com.carrotsearch.hppc.procedures.LongIntProcedure;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.container.Paths;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.BfsSources;
import org.neo4j.graphalgo.impl.msbfs.BfsWithPredecessorConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MSBetweennessCentrality extends Algorithm<MSBetweennessCentrality, AtomicDoubleArray> {

    private final Graph graph;
    private final int nodeCount;
    private final boolean undirected;
    private final int bfsCount;
    private final int concurrency;
    private final ExecutorService executorService;
    private final AllocationTracker tracker;

    private final AtomicDoubleArray centrality;

    public MSBetweennessCentrality(
        Graph graph,
        boolean undirected,
        int bfsCount,
        ExecutorService executorService,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.undirected = undirected;
        this.bfsCount = bfsCount;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.tracker = tracker;
        this.centrality = new AtomicDoubleArray(nodeCount);
    }

    private Queue<MSBetweennessCentralityConsumer> initConsumerPool() {
        ArrayBlockingQueue<MSBetweennessCentralityConsumer> queue = new ArrayBlockingQueue<>(concurrency);

        for (int i = 0; i < concurrency; i++) {
            queue.offer(new MSBetweennessCentralityConsumer(
                bfsCount,
                nodeCount,
                centrality,
                undirected ? 2.0 : 1.0
            ));
        }

        return queue;
    }

    @Override
    public AtomicDoubleArray compute() {
        var taskCount = (int) ParallelUtil.threadCount(bfsCount, graph.nodeCount());
        var consumerPool = initConsumerPool();
        var multiSourceBFS = MultiSourceBFS.predecessorProcessing(graph, tracker);

        var taskProvider = new MSBetweennessCentrality.TaskProvider(
            multiSourceBFS,
            graph.nodeCount(),
            taskCount,
            bfsCount,
            consumerPool
        );

        ParallelUtil.runWithConcurrency(
            concurrency,
            taskProvider,
            taskCount << 2,
            100L,
            TimeUnit.MICROSECONDS,
            executorService
        );

        return centrality;
    }

    @Override
    public MSBetweennessCentrality me() {
        return this;
    }

    @Override
    public void release() {

    }

    public AtomicDoubleArray getCentrality() {
        return centrality;
    }

    public Stream<Result> resultStream() {
        return IntStream
            .range(0, nodeCount)
            .mapToObj(nodeId ->
                new Result(
                    graph.toOriginalNodeId(nodeId),
                    centrality.get(nodeId)));
    }

    static final class TaskProvider extends AbstractCollection<MSBetweennessCentralityTask> implements Iterator<MSBetweennessCentralityTask> {

        private final MultiSourceBFS multiSourceBFS;
        private final long nodeCount;
        private final int taskCount;
        private final int bfsCount;
        private final Queue<MSBetweennessCentralityConsumer> consumerPool;

        private long offset = 0L;
        private int currentTask = 0;

        TaskProvider(
            MultiSourceBFS multiSourceBFS,
            long nodeCount,
            int taskCount,
            int bfsCount,
            Queue<MSBetweennessCentralityConsumer> consumerPool
        ) {
            this.multiSourceBFS = multiSourceBFS;
            this.nodeCount = nodeCount;
            this.taskCount = taskCount;
            this.bfsCount = bfsCount;
            this.consumerPool = consumerPool;
        }

        @Override
        public Iterator<MSBetweennessCentralityTask> iterator() {
            offset = 0L;
            currentTask = 0;
            return this;
        }

        @Override
        public int size() {
            return taskCount;
        }

        @Override
        public boolean hasNext() {
            return currentTask < taskCount;
        }

        @Override
        public MSBetweennessCentralityTask next() {
            var limit = Math.min(offset + bfsCount, nodeCount);
            var startNodes = LongStream.range(offset, limit).toArray();
            offset += startNodes.length;
            currentTask++;
            return new MSBetweennessCentralityTask(multiSourceBFS, startNodes, consumerPool);
        }
    }

    static final class MSBetweennessCentralityTask implements Runnable {

        private final MultiSourceBFS multiSourceBFS;
        private final long[] startNodes;
        private final Queue<MSBetweennessCentralityConsumer> consumerPool;

        MSBetweennessCentralityTask(
            MultiSourceBFS multiSourceBFS,
            long[] startNodes,
            Queue<MSBetweennessCentralityConsumer> consumerPool
        ) {
            this.multiSourceBFS = multiSourceBFS;
            this.startNodes = startNodes;
            this.consumerPool = consumerPool;
        }

        @Override
        public void run() {
            // pick a consumer from the pool
            var consumer = consumerPool.poll();
            //noinspection ConstantConditions
            consumer.init(startNodes);
            // concurrent forward traversal for all start nodes
            multiSourceBFS.initPredecessorProcessing(consumer, consumer, startNodes).run();
            // sequential backward traversal for all start nodes
            consumer.updateCentrality();
            // release the consumer back to the pool
            consumerPool.offer(consumer);
        }
    }

    static final class MSBetweennessCentralityConsumer implements BfsConsumer, BfsWithPredecessorConsumer {

        private final LongIntScatterMap idMapping;
        private final double divisor;
        private final AtomicDoubleArray centrality;

        private final Paths[] paths;
        private final IntStack[] stacks;
        private final double[][] deltas;
        private final int[][] sigmas;
        private final int[][] distances;

        MSBetweennessCentralityConsumer(int bfsCount, int nodeCount, AtomicDoubleArray centrality, double divisor) {
            this.centrality = centrality;
            this.idMapping = new LongIntScatterMap(bfsCount);
            this.paths = new Paths[bfsCount];
            this.stacks = new IntStack[bfsCount];
            this.deltas = new double[bfsCount][];
            this.sigmas = new int[bfsCount][];
            this.distances = new int[bfsCount][];
            this.divisor = divisor;

            for (int i = 0; i < bfsCount; i++) {
                paths[i] = new Paths();
                stacks[i] = new IntStack();
                deltas[i] = new double[nodeCount];
                sigmas[i] = new int[nodeCount];
                distances[i] = new int[nodeCount];
            }
        }

        void init(long[] startNodes) {
            // clear the mapping
            idMapping.clear();

            for (int i = 0; i < startNodes.length; i++) {
                // fill arrays
                Arrays.fill(sigmas[i], 0);
                Arrays.fill(deltas[i], 0);
                Arrays.fill(distances[i], -1);

                // remove results from prior task
                paths[i].clear();
                stacks[i].clear();

                // re-map the new batch of nodes
                idMapping.put(startNodes[i], i);

                // starting values for each node
                sigmas[i][(int) startNodes[i]] = 1;
                distances[i][(int) startNodes[i]] = 0;
            }
        }

        void updateCentrality() {
            idMapping.forEach((LongIntProcedure) (sourceNodeId, localNodeId) -> {
                var localStack = stacks[localNodeId];
                var localPaths = paths[localNodeId];
                var localDelta = deltas[localNodeId];
                var localSigma = sigmas[localNodeId];

                while (!localStack.isEmpty()) {
                    int node = localStack.pop();
                    localPaths.forEach(node, predecessor -> {
                        localDelta[predecessor] += (double) localSigma[predecessor] / (double) localSigma[node] * (localDelta[node] + 1.0);
                        return true;
                    });
                    if (node != sourceNodeId) {
                        centrality.add(node, localDelta[node] / divisor);
                    }
                }
            });
        }

        // Called exactly once if `node` is visited for the first time.
        @Override
        public void accept(long node, int depth, BfsSources startNodes) {
            while (startNodes.hasNext()) {
                stacks[idMapping.get(startNodes.next())].push((int) node);
            }
        }

        // Called if `node` has been discovered by at least one BFS via the `predecessor`.
        // Might be called multiple times for the same BFS, but with different predecessors.
        @Override
        public void accept(long node, long predecessor, int depth, BfsSources startNodes) {
            while (startNodes.hasNext()) {
                accept(node, predecessor, depth, idMapping.get(startNodes.next()));
            }
        }

        private void accept(long node, long predecessor, int depth, int startNode) {
            int source = (int) predecessor;
            int target = (int) node;
            // target found for the first time?
            int[] distance = distances[startNode];
            if (distance[target] < 0) {
                distance[target] = depth;
            }
            // shortest path to target via source?
            if (distance[target] == distance[source] + 1) {
                sigmas[startNode][target] += sigmas[startNode][source];
                paths[startNode].append(target, source);
            }
        }
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        // original node id
        public final long nodeId;
        // centrality value
        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                   "nodeId=" + nodeId +
                   ", centrality=" + centrality +
                   '}';
        }
    }

}
