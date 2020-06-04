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
package org.neo4j.graphalgo.impl.betweenness;

import com.carrotsearch.hppc.IntStack;
import com.carrotsearch.hppc.LongIntScatterMap;
import com.carrotsearch.hppc.procedures.LongIntProcedure;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.container.Paths;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.BfsSources;
import org.neo4j.graphalgo.impl.msbfs.BfsWithPredecessorConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;
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

    @Override
    public AtomicDoubleArray compute() {
        var taskCount = (int) ParallelUtil.threadCount(bfsCount, graph.nodeCount());
        var taskProvider = new MSBetweennessCentrality.TaskProvider(
            graph,
            taskCount,
            bfsCount,
            centrality,
            undirected,
            tracker
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

    public Stream<BetweennessCentrality.Result> resultStream() {
        return IntStream
            .range(0, nodeCount)
            .mapToObj(nodeId ->
                new BetweennessCentrality.Result(
                    graph.toOriginalNodeId(nodeId),
                    centrality.get(nodeId)));
    }

    static final class TaskProvider extends AbstractCollection<MSBetweennessCentralityTask> implements Iterator<MSBetweennessCentralityTask> {

        private final Graph graph;
        private final int taskCount;
        private final int bfsCount;
        private final AtomicDoubleArray centrality;
        private final boolean undirected;
        private final AllocationTracker tracker;

        private long offset = 0L;
        private int i = 0;

        TaskProvider(
            Graph graph,
            int taskCount,
            int bfsCount,
            AtomicDoubleArray centrality, boolean undirected, AllocationTracker tracker
        ) {
            this.graph = graph;
            this.taskCount = taskCount;
            this.bfsCount = bfsCount;
            this.centrality = centrality;
            this.undirected = undirected;
            this.tracker = tracker;
        }

        @Override
        public Iterator<MSBetweennessCentralityTask> iterator() {
            offset = 0L;
            i = 0;
            return this;
        }

        @Override
        public int size() {
            return taskCount;
        }

        @Override
        public boolean hasNext() {
            return i < taskCount;
        }

        @Override
        public MSBetweennessCentralityTask next() {
            var limit = Math.min(offset + bfsCount, graph.nodeCount());
            var startNodes = LongStream.range(offset, limit).toArray();
            offset += startNodes.length;
            i++;
            return new MSBetweennessCentralityTask(graph.concurrentCopy(), startNodes, centrality, undirected, tracker);
        }
    }

    static final class MSBetweennessCentralityTask implements Runnable {

        private final Graph graph;

        private final long[] startNodes;

        private final MSBCBFSConsumer consumer;

        private final AllocationTracker tracker;

        MSBetweennessCentralityTask(
            Graph graph,
            long[] startNodes,
            AtomicDoubleArray centrality,
            boolean undirected,
            AllocationTracker tracker
        ) {
            this.graph = graph;
            this.startNodes = startNodes;
            int nodeCount = Math.toIntExact(graph.nodeCount());
            this.consumer = new MSBCBFSConsumer(startNodes.length, nodeCount, centrality, undirected ? 2.0 : 1.0);
            this.tracker = tracker;
        }

        @Override
        public void run() {
            consumer.init(startNodes, false);
            // concurrent forward traversal for all start nodes
            MultiSourceBFS
                .predecessorProcessing(graph, graph, consumer, consumer, tracker, startNodes)
                .run();
            // sequential backward traversal for all start nodes
            consumer.updateCentrality();
        }
    }

    static final class MSBCBFSConsumer implements BfsConsumer, BfsWithPredecessorConsumer {

        private final LongIntScatterMap idMapping;
        private final double divisor;
        private final AtomicDoubleArray centrality;

        private final Paths[] paths;
        private final IntStack[] stacks;
        private final double[][] deltas;
        private final int[][] sigmas;
        private final int[][] distances;

        MSBCBFSConsumer(int bfsCount, int nodeCount, AtomicDoubleArray centrality, double divisor) {
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

        void init(long[] startNodes, boolean clear) {
            for (int i = 0; i < startNodes.length; i++) {
                if (clear) {
                    Arrays.fill(sigmas[i], 0);
                    Arrays.fill(deltas[i], 0);
                    idMapping.clear();
                    paths[i].clear();
                    stacks[i].clear();
                }
                idMapping.put(startNodes[i], i);
                Arrays.fill(distances[i], -1);
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
}
