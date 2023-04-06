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

import com.carrotsearch.hppc.LongArrayList;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A task that will compute BFS for portion of the graph.
 *
 * Computation is performed in parallel while synchronization of the result is done sequentially.
 */
class BFSTask implements Runnable {

    private static final int CHUNK_SEPARATOR = -1;

    // shared variables; see comments in `BFS`.
    private final Graph graph;
    private final AtomicLong traversedNodesIndex;
    private final HugeAtomicBitSet visited;
    private final HugeLongArray traversedNodes;
    private final HugeDoubleArray weights;
    private final AtomicLong targetFoundIndex;
    private final AtomicLong traversedNodesLength;
    private final HugeAtomicLongArray minimumChunk;
    private final ExitPredicate exitPredicate;
    private final Aggregator aggregatorFunction;
    private final long sourceNodeId;

    // variables local to the task
    // Chunk(s) of `traversedNodes` that a single task operates on; each chunk
    // ends with `BFSTask.CHUNK_SEPARATOR` which acts as a separator.
    private final LongArrayList localNodes;

    // IDs of the chunk(s) processed by a single task, ordered in ascending order.
    private final LongArrayList chunks;
    // Maximum size of a single chunk.
    private final int delta;

    private final TerminationFlag terminationFlag;

    // Used in the synchronization phase, keeps track of the current chunk index.
    private int indexOfChunk;

    // Used in the synchronization phase, keeps track of the current index in the `localNodes`.
    private int indexOfLocalNodes;

    private final ProgressTracker progressTracker;

    BFSTask(
        Graph graph,
        HugeLongArray traversedNodes,
        AtomicLong traversedNodesIndex,
        AtomicLong traversedNodesLength,
        HugeAtomicBitSet visited,
        HugeDoubleArray weights,
        AtomicLong targetFoundIndex,
        HugeAtomicLongArray minimumChunk,
        ExitPredicate exitPredicate,
        Aggregator aggregatorFunction,
        int delta,
        long sourceNodeId,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.graph = graph.concurrentCopy();
        this.traversedNodesIndex = traversedNodesIndex;
        this.traversedNodesLength = traversedNodesLength;
        this.visited = visited;
        this.traversedNodes = traversedNodes;
        this.weights = weights;
        this.targetFoundIndex = targetFoundIndex;
        this.minimumChunk = minimumChunk;
        this.exitPredicate = exitPredicate;
        this.aggregatorFunction = aggregatorFunction;
        this.delta = delta;
        this.sourceNodeId = sourceNodeId;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;

        this.localNodes = new LongArrayList();
        this.chunks = new LongArrayList();
    }

    long currentChunkId() {
        return this.chunks.get(indexOfChunk);
    }

    private void moveToNextChunk() {
        indexOfChunk++;
    }

    boolean hasMoreChunks() {
        return indexOfChunk < chunks.size();
    }

    /**
     * Processes a single chunk at a time.
     */
    public void run() {
        resetChunks();

        long examined = 0L;
        long offset;
        while ((offset = traversedNodesIndex.getAndAdd(delta)) < traversedNodesLength.get()) {
            var chunkLimit = Math.min(offset + delta, traversedNodesLength.get());
            chunks.add(offset);
            for (long idx = offset; idx < chunkLimit; idx++) {
                var nodeId = traversedNodes.get(idx);
                long sourceId = sourceNodeId;
                double weight = 0;
                if (nodeId != sourceNodeId) {
                    long minimumChunkIndex = minimumChunk.get(nodeId);
                    sourceId = traversedNodes.get(minimumChunkIndex);
                    weight = aggregatorFunction.apply(sourceId, nodeId, weights.get(minimumChunkIndex));
                    weights.set(idx, weight);
                }

                relaxNode(idx, nodeId, sourceId, weight);
                examined++;
            }
            // Add a chunk separator.
            localNodes.add(CHUNK_SEPARATOR);
        }

        progressTracker.logProgress(examined);
    }

    void syncNextChunk() {
        if (!localNodes.isEmpty()) {
            // Keep track on how many nodes are in the current chunk.
            int nodesTraversed = 0;
            long index = traversedNodesLength.get();
            while (localNodes.get(indexOfLocalNodes) != CHUNK_SEPARATOR) {
                long nodeId = localNodes.get(indexOfLocalNodes);
                // The chunks are ordered and processed  sequentially,
                // the first time, we encounter `nodeId` in localNodes,
                // `nodeId` is set to visited and added to traversedNodes.
                // In case `nodeId` is encountered in a later chunk,
                // the if check will be false and not added to traversedNodes again.
                if (!visited.getAndSet(nodeId)) {
                    traversedNodes.set(index, nodeId);
                    index++;
                    nodesTraversed++;
                }
                indexOfLocalNodes++;
            }
            // Account for chunk separator.
            indexOfLocalNodes++;
            // Update the global `traverseNodesLength`.
            traversedNodesLength.getAndAdd(nodesTraversed);
            moveToNextChunk();
            if (!hasMoreChunks()) {
                resetLocalState();
            }
        }
    }

    private void relaxNode(long nodeIndex, long nodeId, long sourceNodeId, double weight) {
        var exitPredicateResult = exitPredicate.test(sourceNodeId, nodeId, weight);
        if (exitPredicateResult == ExitPredicate.Result.BREAK) {
            // Update the global `targetFoundIndex` so other tasks will know a target is reached and terminate as well.
            targetFoundIndex.getAndAccumulate(nodeIndex, Math::min);
            return;
        }

        this.graph.forEachRelationship(
            nodeId,
            (s, targetNodeId) -> {
                // Potential race condition
                // We consider it okay since it's handled by `minimumChunk.update`
                // which is an atomic operation and always will have the minimum chunk index.
                // Due to the fact, that syncing is done sequentially, there will never be a problem
                // even when two different threads include `targetNodeId` in their localNodes.
                // This is further explained in the syncNextChunk function.
                if (!visited.get(targetNodeId)) {
                    minimumChunk.update(targetNodeId, currentValue -> Math.min(currentValue, nodeIndex));
                    if (minimumChunk.get(targetNodeId) == nodeIndex) {
                        localNodes.add(targetNodeId);
                    }
                }
                return terminationFlag.running();
            }
        );
    }

    private void resetLocalState() {
        localNodes.elementsCount = 0;
        indexOfLocalNodes = 0;
    }

    private void resetChunks() {
        chunks.elementsCount = 0;
        indexOfChunk = 0;
    }
}
