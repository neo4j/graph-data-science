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
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.container.Paths;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.BfsSources;
import org.neo4j.graphalgo.impl.msbfs.BfsWithPredecessorConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class MSBetweennessCentrality extends Algorithm<MSBetweennessCentrality, AtomicDoubleArray> {

    private final Graph graph;
    private final int nodeCount;
    private final boolean undirected;
    private final int concurrentBfs;
    private final int concurrency;
    private final ExecutorService executorService;
    private final AllocationTracker tracker;

    private final AtomicDoubleArray centrality;

    public MSBetweennessCentrality(
        Graph graph,
        boolean undirected,
        int concurrentBfs,
        ExecutorService executorService,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.nodeCount = Math.toIntExact(graph.nodeCount());
        this.undirected = undirected;
        this.concurrentBfs = concurrentBfs;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.tracker = tracker;
        this.centrality = new AtomicDoubleArray(nodeCount);
    }

    @Override
    public AtomicDoubleArray compute() {
        var consumer = new MSBCBFSConsumer(concurrentBfs, nodeCount, centrality, undirected ? 2.0 : 1.0);

        // TODO: parallelize over node batches
        for (long offset = 0; offset < nodeCount; offset += concurrentBfs) {
            var limit = Math.min(offset + concurrentBfs, nodeCount);
            var startNodes = LongStream.range(offset, limit).toArray();
            consumer.init(startNodes, offset > 0);
            // forward traversal for all start nodes
            MultiSourceBFS
                .predecessorProcessing(graph, graph, consumer, consumer, tracker, startNodes)
                .run(concurrency, executorService);
            // backward traversal for all start nodes
            consumer.updateCentrality(startNodes);
        }

        return centrality;
    }

    @Override
    public MSBetweennessCentrality me() {
        return this;
    }

    @Override
    public void release() {

    }

    public Stream<BetweennessCentrality.Result> resultStream() {
        return IntStream
            .range(0, nodeCount)
            .mapToObj(nodeId ->
                new BetweennessCentrality.Result(
                    graph.toOriginalNodeId(nodeId),
                    centrality.get(nodeId)));
    }

    static class MSBCBFSConsumer implements BfsConsumer, BfsWithPredecessorConsumer {

        private final LongIntMap idMapping;
        private final int localNodeCount;
        private final double divisor;
        private final AtomicDoubleArray centrality;

        private final Paths[] paths;
        private final IntStack[] stacks;
        private final double[][] deltas;
        private final int[][] sigmas;
        private final int[][] distances;

        MSBCBFSConsumer(int localNodeCount, int nodeCount, AtomicDoubleArray centrality, double divisor) {
            this.localNodeCount = localNodeCount;
            this.centrality = centrality;

            this.idMapping = new LongIntHashMap(localNodeCount);
            this.paths = new Paths[localNodeCount];
            this.stacks = new IntStack[localNodeCount];
            this.deltas = new double[localNodeCount][];
            this.sigmas = new int[localNodeCount][];
            this.distances = new int[localNodeCount][];
            this.divisor = divisor;

            for (int i = 0; i < localNodeCount; i++) {
                paths[i] = new Paths();
                stacks[i] = new IntStack();
                deltas[i] = new double[nodeCount];
                sigmas[i] = new int[nodeCount];
                distances[i] = new int[nodeCount];
            }
        }

        void init(long[] sourceNodes, boolean clear) {
            if (sourceNodes.length > localNodeCount) {
                throw new IllegalArgumentException("too many source nodes");
            }

            for (int i = 0; i < sourceNodes.length; i++) {
                if (clear) {
                    Arrays.fill(sigmas[i], 0);
                    Arrays.fill(deltas[i], 0);
                    idMapping.clear();
                    paths[i].clear();
                    stacks[i].clear();
                }
                idMapping.put(sourceNodes[i], i);
                Arrays.fill(distances[i], -1);
                sigmas[i][(int) sourceNodes[i]] = 1;
                distances[i][(int) sourceNodes[i]] = 0;
            }
        }

        void updateCentrality(long[] sourceNodes) {
            // TODO: parallelize
            for (long sourceNode : sourceNodes) {
                int startNodeId = idMapping.get(sourceNode);
                var localStack = stacks[startNodeId];
                var localPaths = paths[startNodeId];
                var localDelta = deltas[startNodeId];
                var localSigma = sigmas[startNodeId];

                while (!localStack.isEmpty()) {
                    int node = localStack.pop();
                    localPaths.forEach(node, v -> {
                        localDelta[v] += (double) localSigma[v] / (double) localSigma[node] * (localDelta[node] + 1.0);
                        return true;
                    });
                    if (node != sourceNode) {
                        centrality.add(node, localDelta[node] / divisor);
                    }
                }
            }
        }

        // Called exactly once if a node is visited for the first time.
        @Override
        public void accept(long nodeId, int depth, BfsSources sourceNodeIds) {
            while (sourceNodeIds.hasNext()) {
                stacks[idMapping.get(sourceNodeIds.next())].push((int) nodeId);
            }
        }

        // Called if nodeId has been discovered by at least one BFS via the predecessorId
        // Might be called multiple times for the same BFS, but with different predecessors.
        @Override
        public void accept(long nodeId, long predecessorId, int depth, BfsSources sourceNodeIds) {
            while (sourceNodeIds.hasNext()) {
                accept(nodeId, predecessorId, depth, idMapping.get(sourceNodeIds.next()));
            }
        }

        private void accept(long nodeId, long predecessorId, int depth, int startNodeId) {
            // This will break for very large graphs
            int source = (int) predecessorId;
            int target = (int) nodeId;
            // target found for the first time?
            int[] distance = distances[startNodeId];
            if (distance[target] < 0) {
                distance[target] = depth;
            }
            // shortest path to target via source?
            if (distance[target] == distance[source] + 1) {
                sigmas[startNodeId][target] += sigmas[startNodeId][source];
                paths[startNodeId].append(target, source);
            }
        }
    }
}
