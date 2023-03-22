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
package org.neo4j.gds.paths.traverse;


import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Parallel implementation of the BFS algorithm.
 *
 * It uses the concept of bucketing/chunking to keep track of the ordering of the
 * visited nodes.
 *
 * Conceptually, a bucket keeps all nodes at a fixed distance from the starting node.
 * The nodes within each bucket are kept in a list ordered by their final position
 * in the output BFS ordering.
 *
 * To implement parallelism, the nodes within a bucket are processed concurrently.
 * For this, the nodes of the bucket are partitioned into chunks, where each chunk
 * contains a continuous segment from the list of nodes. Threads are then assigned
 * chunks in parallel and process (relax) each node within the assigned chunk.
 *
 * To maintain a correct ordering, once the parallel processing phase has concluded,
 * we perform a sequential step, where we examine the chunks from earliest to latest
 * to create the next bucket, such that a correct BFS ordering is returned where all
 * descendants from the nodes of a chunk, appear together before those from a later
 * chunk.
 */
public final class BFS extends Algorithm<HugeLongArray> {

    private static final int DEFAULT_DELTA = 64;
    public static final int ALL_DEPTHS_ALLOWED = -1;

    private final long sourceNodeId;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private final Graph graph;
    private final int delta;
    private final long maximumDepth;
    // An array to keep the node ids that were already traversed in the correct order.
    // It is initialized with the total number of nodes, but may contain less than that.
    private HugeLongArray traversedNodes;

    // An array to keep the weight/depth of the node at the same position in `traversedNodes`.
    // It is initialized with the total number of nodes, but may contain less than that.
    // This is used for early termination when `maxDepth` parameter is specified.
    // `maxDepth` specifies the number of "layers" that will be traversed in the input graph,
    // starting from `startNodeId`.
    private HugeDoubleArray weights;

    // Used to keep track of the visited nodes, the value at each index will be `true` for
    // each node id in the `traversedNodes`.
    private HugeAtomicBitSet visited;

    private final int concurrency;

    public static BFS create(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int concurrency,
        ProgressTracker progressTracker,
        long maximumDepth
    ) {
        return create(
            graph,
            startNodeId,
            exitPredicate,
            aggregatorFunction,
            concurrency,
            progressTracker,
            DEFAULT_DELTA,
            maximumDepth
        );
    }

    static BFS create(
        Graph graph,
        long startNodeId,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int concurrency,
        ProgressTracker progressTracker,
        int delta,
        long maximumDepth
    ) {

        var nodeCount = graph.nodeCount();

        var traversedNodes = HugeLongArray.newArray(nodeCount);
        var weights = HugeDoubleArray.newArray(nodeCount);
        var visited = HugeAtomicBitSet.create(nodeCount);

        return new BFS(
            graph,
            startNodeId,
            traversedNodes,
            weights,
            visited,
            exitPredicate,
            aggregatorFunction,
            concurrency,
            progressTracker,
            delta,
            maximumDepth
        );
    }

    private BFS(
        Graph graph,
        long sourceNodeId,
        HugeLongArray traversedNodes,
        HugeDoubleArray weights,
        HugeAtomicBitSet visited,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int concurrency,
        ProgressTracker progressTracker,
        int delta,
        long maximumDepth
    ) {
        super(progressTracker);
        this.graph = graph;
        this.sourceNodeId = sourceNodeId;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.concurrency = concurrency;
        this.delta = delta;
        this.maximumDepth = maximumDepth;
        this.traversedNodes = traversedNodes;
        this.weights = weights;
        this.visited = visited;
    }

    @Override
    public HugeLongArray compute() {
        progressTracker.beginSubTask(graph.relationshipCount());

        // This is used to read from `traversedNodes` in chunks, updated in `BFSTask`.
        var traversedNodesIndex = new AtomicLong(0);
        // This keeps the current length of the `traversedNodes`, updated in `BFSTask.syncNextChunk`.
        var traversedNodesLength = new AtomicLong(1);
        // Used for early exit when target node is reached (if specified by the user), updated in `BFSTask`.
        var targetFoundIndex = new AtomicLong(Long.MAX_VALUE);

        // The minimum position of a predecessor that contains a relationship to the node in the `traversedNodes`.
        // This is updated in `BFSTask` and is helping to maintain the correct traversal order for the output.
        var minimumChunk = HugeAtomicLongArray.of(
            graph.nodeCount(),
            ParalleLongPageCreator.of(concurrency, l -> Long.MAX_VALUE)
        );

        visited.set(sourceNodeId);
        traversedNodes.set(0, sourceNodeId);
        weights.set(0, 0);

        var bfsTaskList = initializeBfsTasks(
            traversedNodesIndex,
            traversedNodesLength,
            targetFoundIndex,
            minimumChunk,
            delta
        );
        int bfsTaskListSize = bfsTaskList.size();
        long currentDepth = 0;
        while (terminationFlag.running()) {
            if (currentDepth == maximumDepth) {
                break;
            }
            ParallelUtil.run(bfsTaskList, Pools.DEFAULT);

            if (targetFoundIndex.get() != Long.MAX_VALUE) {
                break;
            }

            // Synchronize the results sequentially
            var previousTraversedNodesLength = traversedNodesLength.get();
            int numberOfFinishedTasks = 0;
            int numberOfTasksWithChunks = countTasksWithChunks(bfsTaskList);
            while (numberOfFinishedTasks != numberOfTasksWithChunks && terminationFlag.running()) {
                int minimumTaskIndex = -1;
                for (int bfsTaskIndex = 0; bfsTaskIndex < bfsTaskListSize; ++bfsTaskIndex) {
                    var currentBfsTask = bfsTaskList.get(bfsTaskIndex);
                    if (currentBfsTask.hasMoreChunks()) {
                        if (minimumTaskIndex == -1) {
                            minimumTaskIndex = bfsTaskIndex;
                        } else {
                            if (bfsTaskList.get(minimumTaskIndex).currentChunkId() > currentBfsTask.currentChunkId()) {
                                minimumTaskIndex = bfsTaskIndex;
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
            currentDepth++;
        }

        // Find the portion of `traversedNodes` that contains the actual result, doesn't account for target node, hence the `if` statement.
        var nodesLengthToRetain = traversedNodesLength.get();
        if (targetFoundIndex.get() != Long.MAX_VALUE) {
            nodesLengthToRetain = targetFoundIndex.longValue() + 1;
        }

        var result = traversedNodes.copyOf(nodesLengthToRetain);

        progressTracker.endSubTask();
        return result;
    }

    private List<BFSTask> initializeBfsTasks(
        AtomicLong traversedNodesIndex,
        AtomicLong traversedNodesLength,
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
                weights,
                targetFoundIndex,
                minimumChunk,
                exitPredicate,
                aggregatorFunction,
                delta,
                sourceNodeId,
                terminationFlag,
                progressTracker
            ));
        }
        return bfsTaskList;
    }

    private int countTasksWithChunks(Collection<BFSTask> bfsTaskList) {
        return (int) bfsTaskList.stream().filter(BFSTask::hasMoreChunks).count();
    }
}
