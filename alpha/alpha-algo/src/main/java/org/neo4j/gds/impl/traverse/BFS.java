/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.impl.traverse;


import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.impl.Converters.longToIntConsumer;

public final class BFS extends Algorithm<long[]> {

    private static final int DEFAULT_DELTA = 64;
    private static final int IGNORE_NODE = -1;

    private final long startNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private final Graph graph;
    private final int delta;

    private HugeLongArray traversedNodes;
    private final HugeLongArray sources;
    private HugeDoubleArray weights;

    private HugeAtomicBitSet visited;
    private final int concurrency;

    BFS(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int concurrency,
        ProgressTracker progressTracker,
        int delta
    ) {
        super(progressTracker);
        this.graph = graph;
        this.startNodeId = startNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.concurrency = concurrency;
        this.delta = delta;

        var nodeCount = Math.toIntExact(graph.nodeCount());

        this.traversedNodes = HugeLongArray.newArray(nodeCount);
        this.sources = HugeLongArray.newArray(nodeCount);
        this.weights = HugeDoubleArray.newArray(nodeCount);
        this.visited = HugeAtomicBitSet.create(nodeCount);
    }

    public BFS(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        this(graph, startNodeId, exitPredicate, aggregatorFunction, concurrency, progressTracker, DEFAULT_DELTA);
    }

    @Override
    public long[] compute() {
        progressTracker.beginSubTask();

        var traversedNodesIndex = new AtomicInteger(0);
        var traversedNodesLength = new AtomicInteger(1);
        var targetFoundIndex = new AtomicLong(Long.MAX_VALUE);

        var minimumChunk = HugeAtomicLongArray.newArray(graph.nodeCount());
        minimumChunk.setAll(Long.MAX_VALUE);

        visited.set(startNodeId);
        traversedNodes.set(0, startNodeId);
        sources.set(0, startNodeId);
        weights.set(0, 0);

        var bfsTaskList = new ArrayList<BFSTask>(concurrency);
        for (int i = 0; i < concurrency; ++i) {
            bfsTaskList.add(new BFSTask(
                graph,
                traversedNodes,
                traversedNodesIndex,
                traversedNodesLength,
                visited,
                sources,
                weights,
                targetFoundIndex,
                minimumChunk
            ));
        }

        while (running()) {
            ParallelUtil.run(bfsTaskList, Pools.DEFAULT);
            if (targetFoundIndex.get() != Long.MAX_VALUE) {
                break;
            }

            int previousTraversedNodesLength = traversedNodesLength.get();

            int tasksFinished = 0;
            int tasksWithChunks = countTasksWithChunks(bfsTaskList);
            while (tasksFinished != tasksWithChunks) {
                int minimumTaskIndex = -1;
                for (int i = 0; i < concurrency; ++i) {
                    var currentBfsTask = bfsTaskList.get(i);
                    if (currentBfsTask.hasMoreChunks()) {
                        if (minimumTaskIndex == -1) {
                            minimumTaskIndex = i;
                        } else {
                            if (bfsTaskList.get(minimumTaskIndex).currentChunkId() > currentBfsTask.currentChunkId()) {
                                minimumTaskIndex = i;
                            }
                        }
                    }
                }
                var minimumIndexBfsTask = bfsTaskList.get(minimumTaskIndex);
                minimumIndexBfsTask.syncNextChunk();
                if (!minimumIndexBfsTask.hasMoreChunks()) {
                    tasksFinished++;
                }
            }

            if (traversedNodesLength.get() == previousTraversedNodesLength) {
                break;
            }

            traversedNodesIndex.set(previousTraversedNodesLength);
        }

        int nodesLengthToRetain = traversedNodesLength.get();
        if (targetFoundIndex.get() != Long.MAX_VALUE) {
            nodesLengthToRetain = targetFoundIndex.intValue() + 1;
        }
        long[] resultNodes = traversedNodes.copyOf(nodesLengthToRetain).toArray();
        resultNodes = Arrays.stream(resultNodes).filter(node -> node != IGNORE_NODE).toArray();
        progressTracker.endSubTask();
        return resultNodes;
    }

    @Override
    public void release() {
        traversedNodes = null;
        weights = null;
        visited = null;
    }

    private int countTasksWithChunks(List<BFSTask> bfsTaskList) {
        int tasksWithChunks = 0;
        for (int i = 0; i < concurrency; ++i) {
            if (bfsTaskList.get(i).hasMoreChunks()) {
                tasksWithChunks++;
            }
        }
        return tasksWithChunks;
    }

    private class BFSTask implements Runnable {
        // shared variables
        private final Graph graph;
        private final AtomicInteger traversedNodesIndex;
        private final HugeAtomicBitSet visited;
        private final HugeLongArray traversedNodes;
        private final HugeLongArray sources;
        private final HugeDoubleArray weights;
        private final AtomicLong targetFoundIndex;
        private final AtomicInteger traversedNodesLength;
        private final HugeAtomicLongArray minimumChunk;

        // variables local to the task
        private final LongArrayList localNodes;
        private final LongArrayList localSources;
        private final DoubleArrayList localWeights;
        private final LongArrayList chunks;
        private int indexOfChunk;
        private int indexOfLocalNodes;

        BFSTask(
            Graph graph,
            HugeLongArray traversedNodes,
            AtomicInteger traversedNodesIndex,
            AtomicInteger traversedNodesLength,
            HugeAtomicBitSet visited,
            HugeLongArray sources,
            HugeDoubleArray weights,
            AtomicLong targetFoundIndex,
            HugeAtomicLongArray minimumChunk
        ) {
            this.graph = graph.concurrentCopy();
            this.traversedNodesIndex = traversedNodesIndex;
            this.traversedNodesLength = traversedNodesLength;
            this.visited = visited;
            this.traversedNodes = traversedNodes;
            this.sources = sources;
            this.weights = weights;
            this.targetFoundIndex = targetFoundIndex;
            this.minimumChunk = minimumChunk;

            this.localNodes = new LongArrayList();
            this.localSources = new LongArrayList();
            this.localWeights = new DoubleArrayList();
            this.chunks = new LongArrayList();
        }

        long currentChunkId() {
            return this.chunks.get(indexOfChunk);
        }

        void moveToNextChunk() {
            indexOfChunk++;
        }

        boolean hasMoreChunks() {
            return indexOfChunk < chunks.size();
        }

        public void run() {
            chunks.elementsCount = 0;
            indexOfChunk = 0;

            int offset;
            while ((offset = traversedNodesIndex.getAndAdd(delta)) < traversedNodesLength.get()) {
                int chunkLimit = Math.min(offset + delta, traversedNodesLength.get());
                chunks.add(offset);
                for (int idx = offset; idx < chunkLimit; idx++) {
                    var nodeId = traversedNodes.get(idx);
                    var sourceId = sources.get(idx);
                    var weight = weights.get(idx);
                    relaxNode(offset, idx, nodeId, sourceId, weight);
                }
                localNodes.add(Long.MAX_VALUE);
                localSources.add(Long.MAX_VALUE);
                localWeights.add(Long.MAX_VALUE);
            }
        }

        void syncNextChunk() {
            if (!localNodes.isEmpty()) {
                int nodesTraversed = 0;
                int index = traversedNodesLength.get();
                while (localNodes.get(indexOfLocalNodes) != Long.MAX_VALUE) {
                    long nodeId = localNodes.get(indexOfLocalNodes);
                    if (minimumChunk.get(nodeId) == currentChunkId()) {
                        traversedNodes.set(index, localNodes.get(indexOfLocalNodes));
                        minimumChunk.set(nodeId, -1);
                        sources.set(index, localSources.get(indexOfLocalNodes));
                        weights.set(index++, localWeights.get(indexOfLocalNodes));
                        visited.set(nodeId);
                        nodesTraversed++;
                    }
                    indexOfLocalNodes++;
                }
                indexOfLocalNodes++;
                traversedNodesLength.getAndAdd(nodesTraversed);
                moveToNextChunk();
                if (!hasMoreChunks()) {
                    localNodes.elementsCount = 0;
                    localSources.elementsCount = 0;
                    localWeights.elementsCount = 0;
                    indexOfLocalNodes = 0;
                }
            }
        }

        void relaxNode(int chunk, int nodeIndex, long nodeId, long sourceNodeId, double weight) {
            var exitPredicateResult = exitPredicate.test(sourceNodeId, nodeId, weight);
            if (exitPredicateResult == ExitPredicate.Result.CONTINUE) {
                traversedNodes.set(nodeIndex, IGNORE_NODE);
                return;
            } else {
                if (exitPredicateResult == ExitPredicate.Result.BREAK) {
                    targetFoundIndex.getAndAccumulate(nodeIndex, Math::min);
                    return;
                }
            }

            this.graph.forEachRelationship(
                nodeId,
                longToIntConsumer((s, t) -> {
                    if (!visited.get(t)) {
                        minimumChunk.update(t, r -> Math.min(r, chunk));
                        if (minimumChunk.get(t) == chunk) {
                            localSources.add(s);
                            localNodes.add(t);
                            localWeights.add(aggregatorFunction.apply(s, t, weight));
                        }
                    }
                    return running();
                })
            );
        }
    }
}
