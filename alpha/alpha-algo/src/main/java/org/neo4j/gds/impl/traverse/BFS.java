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

public final class BFS extends Algorithm<long[]> {

    private static final int DEFAULT_DELTA = 64;
    static final int IGNORE_NODE = -1;

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

        var bfsTaskList = initialiseBfsTasks(
            traversedNodesIndex,
            traversedNodesLength,
            targetFoundIndex,
            minimumChunk,
            delta
        );

        while (running()) {
            ParallelUtil.run(bfsTaskList, Pools.DEFAULT);
            if (targetFoundIndex.get() != Long.MAX_VALUE) {
                break;
            }

            int previousTraversedNodesLength = traversedNodesLength.get();

            int numberOfFinishedTasks = 0;
            int numberOfTasksWithChunks = countTasksWithChunks(bfsTaskList);
            while (numberOfFinishedTasks != numberOfTasksWithChunks) {
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
                    numberOfFinishedTasks++;
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

    private List<BFSTask> initialiseBfsTasks(
        AtomicInteger traversedNodesIndex,
        AtomicInteger traversedNodesLength,
        AtomicLong targetFoundIndex,
        HugeAtomicLongArray minimumChunk,
        int delta
    ) {
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
                minimumChunk,
                exitPredicate,
                aggregatorFunction,
                delta,
                terminationFlag
            ));
        }
        return bfsTaskList;
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

}
