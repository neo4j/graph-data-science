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
package org.neo4j.gds.triangle;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.mem.Estimate.sizeOfInstance;
import static org.neo4j.gds.mem.Estimate.sizeOfIntArray;
import static org.neo4j.gds.mem.Estimate.sizeOfLongArray;

/**
 * Triangle Centrality identifies important vertices based on the concentration
 * of triangles surrounding each vertex.
 *
 * Based on Paul Burkhardt's algorithm (DOI:10.1145/3685677).
 *
 * TC(v) = ((1/3) * Σ_{u ∈ N△[v]} △(u) + Σ_{w ∈ N(v)\N△(v)} △(w)) / △(G)
 */
public final class TriangleCentrality implements Algorithm<TriangleCentralityResult> {

    static final int SOURCE_EDGE_BATCH_BITS = 1 << 16;
    private static final int SOURCE_EDGE_BATCH_WORDS = SOURCE_EDGE_BATCH_BITS / Long.SIZE;
    private static final int SUM_TASK_BATCH_SIZE = 64;

    private final Graph graph;
    private final Concurrency concurrency;
    private final long maxDegree;
    private final LabelFilterChecker labelFilterChecker;
    private final ExecutorService executorService;
    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    private AdjacencyEntryIndex adjacencyEntryIndex;
    private HugeAtomicBitSet triangleEdges;
    private HugeAtomicLongArray triangleCounts;
    private final AtomicLong nextNodeId = new AtomicLong();

    static long sourceEdgeBatchMemoryEstimation() {
        // SourceEdgeBatch object: header, array references, and dirtyWordCount.
        return sizeOfInstance(SourceEdgeBatch.class)
            // words: local source-edge bitmap, including array overhead.
            + sizeOfLongArray(SOURCE_EDGE_BATCH_WORDS)
            // dirtyWordIndexes: indexes of modified bitmap words, including array overhead.
            + sizeOfIntArray(SOURCE_EDGE_BATCH_WORDS);
    }

    public static TriangleCentrality create(
        Graph graph,
        Concurrency concurrency,
        long maxDegree,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        return create(
            graph,
            concurrency,
            maxDegree,
            Collections.emptyList(),
            executorService,
            progressTracker,
            terminationFlag
        );
    }

    public static TriangleCentrality create(
        Graph graph,
        Concurrency concurrency,
        long maxDegree,
        List<String> labelFilter,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        return new TriangleCentrality(
            graph,
            concurrency,
            maxDegree,
            labelFilter,
            executorService,
            progressTracker,
            terminationFlag
        );
    }

    private TriangleCentrality(
        Graph graph,
        Concurrency concurrency,
        long maxDegree,
        List<String> labelFilter,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.graph = graph;
        this.concurrency = concurrency;
        this.maxDegree = maxDegree;
        this.labelFilterChecker = new LabelFilterChecker(labelFilter, graph::hasLabel);
        this.executorService = executorService;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public TriangleCentralityResult compute() {
        long nodeCount = graph.nodeCount();
        if (nodeCount == 0) {
            return TriangleCentralityResult.EMPTY;
        }

        progressTracker.beginSubTask();

        // Phase 0: Index adjacency entry positions without copying the graph's targets.
        progressTracker.beginSubTask();
        adjacencyEntryIndex = AdjacencyEntryIndex.create(graph, maxDegree, concurrency, progressTracker, terminationFlag);
        long totalAdjacencyEntries = adjacencyEntryIndex.adjacencyEntryCount();
        progressTracker.endSubTask();

        // Phase 1: Count triangles and mark their edges.
        progressTracker.beginSubTask();
        triangleCounts = HugeAtomicLongArray.of(nodeCount, ParalleLongPageCreator.passThrough(concurrency));
        // Match Triangle Count's exclusion sentinel using the selected graph's degree. A node can
        // still be omitted from intersections because its degree before node filtering is larger.
        if (maxDegree < Long.MAX_VALUE) {
            ParallelUtil.parallelForEachNode(nodeCount, concurrency, terminationFlag, node -> {
                if (graph.degree(node) > maxDegree) {
                    triangleCounts.set(node, IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT);
                }
            });
        }
        triangleEdges = HugeAtomicBitSet.create(totalAdjacencyEntries);

        nextNodeId.set(0);
        var intersectTasks = new ArrayList<IntersectTask>(concurrency.value());
        for (int i = 0; i < concurrency.value(); i++) {
            intersectTasks.add(new IntersectTask());
        }
        ParallelUtil.run(intersectTasks, executorService);

        long globalTriangleCount = intersectTasks.stream().mapToLong(task -> task.triangleCount).sum();
        progressTracker.endSubTask();

        // Phase 2: Compute neighbor counts and triangle-count sums.
        progressTracker.beginSubTask();

        // Number of neighbors that share a triangle with the node.
        HugeAtomicLongArray triangleNeighbors = HugeAtomicLongArray.of(
            nodeCount, ParalleLongPageCreator.passThrough(concurrency)
        );

        // Total number of neighbors.
        HugeAtomicLongArray allNeighbors = HugeAtomicLongArray.of(
            nodeCount, ParalleLongPageCreator.passThrough(concurrency)
        );

        // Sum of triangle counts over neighbors that share a triangle with the node.
        HugeAtomicLongArray triangleNeighborsTriangleSums = HugeAtomicLongArray.of(
            nodeCount, ParalleLongPageCreator.passThrough(concurrency)
        );

        // Sum of triangle counts over all neighbors.
        HugeAtomicLongArray allNeighborsTriangleSums = HugeAtomicLongArray.of(
            nodeCount, ParalleLongPageCreator.passThrough(concurrency)
        );

        nextNodeId.set(0);
        Collection<? extends Runnable> sumTasks = ParallelUtil.tasks(
            concurrency, () -> new SumTask(
                triangleNeighbors,
                allNeighbors,
                triangleNeighborsTriangleSums,
                allNeighborsTriangleSums
            )
        );
        ParallelUtil.run(sumTasks, executorService);
        progressTracker.endSubTask();

        // The index and edge marks are only needed to find triangles and aggregate neighbor statistics.
        // Centrality calculation uses the per-node arrays below.
        adjacencyEntryIndex = null;
        triangleEdges = null;

        // Phase 3: Compute triangle centrality values
        progressTracker.beginSubTask();
        HugeDoubleArray centralities = HugeDoubleArray.newArray(nodeCount);
        double normalizationFactor = (double) globalTriangleCount;

        ParallelUtil.parallelForEachNode(nodeCount, concurrency, terminationFlag, v -> {
            // Preserve the distinction between excluded nodes and included nodes with no accepted triangles.
            if (triangleCounts.get(v) == IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT) {
                centralities.set(v, Double.NaN);
            } else if (globalTriangleCount == 0) {
                centralities.set(v, 0.0);
            } else {
                long triangleNeighborSum = triangleNeighborsTriangleSums.get(v);
                long closedTriangleNeighborSum = triangleNeighborSum + triangleCounts.get(v);
                long nonTriangleNeighborSum = allNeighborsTriangleSums.get(v) - triangleNeighborSum;
                centralities.set(
                    v, ((1.0 / 3.0) * closedTriangleNeighborSum + nonTriangleNeighborSum) / normalizationFactor
                );
            }
            progressTracker.onProgress();
        });
        progressTracker.endSubTask();

        progressTracker.endSubTask();

        return new TriangleCentralityResult(
            centralities,
            globalTriangleCount,
            triangleCounts,
            triangleNeighbors,
            allNeighbors,
            triangleNeighborsTriangleSums,
            allNeighborsTriangleSums
        );
    }

    /**
     * Keeps Triangle Count's intersection order, but applies {@code labelFilter} only to the counters.
     * Every triangle surviving the degree limit must mark its edges, even when rejected by label
     * filtering, so triangle neighbors cannot move into the full-weight non-triangle term of the formula.
     * Counts are not read until all intersection tasks finish, so source and neighbor contributions
     * can be batched locally. Their final additions remain atomic because other tasks update them too.
     */
    private class IntersectTask implements Runnable {
        private AdjacencyEntryIndex.LowerNeighborCursor lowerNeighbors;
        private AdjacencyEntryIndex.LowerNeighborCursor sourceCandidates;
        private AdjacencyEntryIndex.LowerNeighborCursor neighborCandidates;
        private final RawPositionCursor rawLowerNeighbors = new RawPositionCursor();
        private final RawPositionCursor rawSourceCandidates = new RawPositionCursor();
        private final RawPositionCursor rawNeighborCandidates = new RawPositionCursor();
        private final SourceEdgeBatch sourceEdgeBatch = new SourceEdgeBatch();
        private long triangleCount;

        @Override
        public void run() {
            long node;
            long nodeCount = graph.nodeCount();
            boolean filterByDegree = maxDegree < Long.MAX_VALUE;
            while ((node = nextNodeId.getAndIncrement()) < nodeCount && terminationFlag.running()) {
                if (filterByDegree && adjacencyEntryIndex.degreeForIntersection(node) > maxDegree) {
                    progressTracker.onProgress();
                    continue;
                }
                if (adjacencyEntryIndex.supportsRawPositionTracking()) {
                    intersectHugeGraphNode(node);
                } else {
                    intersectFallbackNode(node);
                }
                progressTracker.onProgress();
            }
        }

        /**
         * For each neighbor below the source ID, scan that neighbor's lower neighbors and advance
         * the source list to the same target. The order {@code third < neighbor < source} discovers each
         * triangle once. Separate outer and candidate cursors let each intersection restart the
         * source list without losing the next neighbor to visit.
         *
         * HugeGraph exposes the raw entry count remaining in each cursor, so this path can recover
         * bitmap positions directly. Batch the two source-owned edge marks, which may recur across
         * many triangles; the third edge belongs to the neighbor's list and is marked globally.
         */
        private void intersectHugeGraphNode(long node) {
            int nodeDegree = graph.degree(node);
            boolean filterByDegree = maxDegree < Long.MAX_VALUE;
            boolean nodeMatchesLabels = labelFilterChecker.check(node);
            long sourceOffset = adjacencyEntryIndex.offset(node);
            long acceptedSourceTriangles = 0;
            rawLowerNeighbors.reset(adjacencyEntryIndex.rawCursorForNode(rawLowerNeighbors.cursor, node), nodeDegree);

            while (rawLowerNeighbors.next() && rawLowerNeighbors.target < node) {
                long neighbor = rawLowerNeighbors.target;
                int neighborDegree = adjacencyEntryIndex.degreeForIntersection(neighbor);
                long neighborOffset = adjacencyEntryIndex.offset(neighbor);
                if (neighborDegree > maxDegree) {
                    continue;
                }
                boolean pairMatchesLabels = nodeMatchesLabels && labelFilterChecker.check(node, neighbor);

                boolean triangleFoundForNeighbor = false;
                long acceptedNeighborTriangles = 0;
                rawSourceCandidates.reset(
                    adjacencyEntryIndex.rawCursorForNode(rawSourceCandidates.cursor, node), nodeDegree
                );
                rawNeighborCandidates.reset(
                    adjacencyEntryIndex.rawCursorForNode(rawNeighborCandidates.cursor, neighbor), neighborDegree
                );

                rawSourceCandidates.next();
                // Sorted source candidates at or above neighbor cannot complete third < neighbor < source.
                while (rawSourceCandidates.target != AdjacencyCursor.NOT_FOUND
                    && rawSourceCandidates.target < neighbor
                    && rawNeighborCandidates.next()
                    && rawNeighborCandidates.target < neighbor) {
                    long thirdNode = rawNeighborCandidates.target;
                    if (!filterByDegree || adjacencyEntryIndex.degreeForIntersection(thirdNode) <= maxDegree) {
                        rawSourceCandidates.advanceTo(thirdNode);
                        if (rawSourceCandidates.target == thirdNode) {
                            if (pairMatchesLabels && labelFilterChecker.check(node, neighbor, thirdNode)) {
                                acceptedNeighborTriangles++;
                                triangleCounts.getAndAdd(thirdNode, 1);
                            }
                            sourceEdgeBatch.mark(rawSourceCandidates.localPosition, sourceOffset, triangleEdges);
                            triangleEdges.set(neighborOffset + rawNeighborCandidates.localPosition);
                            triangleFoundForNeighbor = true;
                        }
                    }
                }

                if (acceptedNeighborTriangles != 0) {
                    triangleCounts.getAndAdd(neighbor, acceptedNeighborTriangles);
                    acceptedSourceTriangles += acceptedNeighborTriangles;
                }
                if (triangleFoundForNeighbor) {
                    sourceEdgeBatch.mark(rawLowerNeighbors.localPosition, sourceOffset, triangleEdges);
                }
            }

            if (acceptedSourceTriangles != 0) {
                triangleCounts.getAndAdd(node, acceptedSourceTriangles);
                triangleCount += acceptedSourceTriangles;
            }
            sourceEdgeBatch.flush(triangleEdges, sourceOffset);
        }

        /**
         * Intersect the source and neighbor lists in sorted order, advancing the smaller candidate
         * until both identify a common third vertex. Requiring {@code third < neighbor < source} gives the
         * same single visit per triangle as the HugeGraph path.
         *
         * Filtered and union views need cursors that preserve adjacency positions while skipping
         * duplicates and degree-excluded neighbors. Their positions are global bitmap indexes;
         * subtract the source offset only when passing a source-owned edge to the local batch.
         */
        private void intersectFallbackNode(long node) {
            long sourceOffset = adjacencyEntryIndex.offset(node);
            boolean nodeMatchesLabels = labelFilterChecker.check(node);
            long acceptedSourceTriangles = 0;
            lowerNeighbors = adjacencyEntryIndex.cursorForNode(lowerNeighbors, node);

            while (lowerNeighbors.hasNext()) {
                long neighbor = lowerNeighbors.target();
                long sourceNeighborGlobalPosition = lowerNeighbors.globalPosition();
                boolean pairMatchesLabels = nodeMatchesLabels && labelFilterChecker.check(node, neighbor);
                boolean triangleFoundForNeighbor = false;
                long acceptedNeighborTriangles = 0;
                sourceCandidates = adjacencyEntryIndex.cursorForNode(sourceCandidates, node);
                neighborCandidates = adjacencyEntryIndex.cursorForNode(neighborCandidates, neighbor);

                // The same upper bound applies to both sorted candidate lists.
                while (sourceCandidates.hasNext()
                    && sourceCandidates.target() < neighbor
                    && neighborCandidates.hasNext()
                    && neighborCandidates.target() < neighbor) {
                    long sourceCandidate = sourceCandidates.target();
                    long neighborCandidate = neighborCandidates.target();
                    if (sourceCandidate == neighborCandidate) {
                        if (pairMatchesLabels && labelFilterChecker.check(node, neighbor, sourceCandidate)) {
                            acceptedNeighborTriangles++;
                            triangleCounts.getAndAdd(sourceCandidate, 1);
                        }
                        sourceEdgeBatch.mark(
                            sourceCandidates.globalPosition() - sourceOffset, sourceOffset, triangleEdges
                        );
                        triangleEdges.set(neighborCandidates.globalPosition());
                        triangleFoundForNeighbor = true;
                        sourceCandidates.next();
                        neighborCandidates.next();
                    } else if (sourceCandidate < neighborCandidate) {
                        sourceCandidates.advanceTo(neighborCandidate);
                    } else {
                        neighborCandidates.next();
                    }
                }

                if (acceptedNeighborTriangles != 0) {
                    triangleCounts.getAndAdd(neighbor, acceptedNeighborTriangles);
                    acceptedSourceTriangles += acceptedNeighborTriangles;
                }
                if (triangleFoundForNeighbor) {
                    sourceEdgeBatch.mark(
                        sourceNeighborGlobalPosition - sourceOffset, sourceOffset, triangleEdges
                    );
                }

                lowerNeighbors.next();
            }

            if (acceptedSourceTriangles != 0) {
                triangleCounts.getAndAdd(node, acceptedSourceTriangles);
                triangleCount += acceptedSourceTriangles;
            }
            sourceEdgeBatch.flush(triangleEdges, sourceOffset);
        }

        /**
         * Yields unique HugeGraph neighbors while retaining each target's first source-relative
         * adjacency position. Counting only unique targets would shift the bitmap indexes after
         * parallel relationships, so positions include every consumed entry, including duplicates.
         * The intersection loops apply ID ordering and degree limits; this cursor only handles
         * traversal and position tracking.
         */
        private final class RawPositionCursor {
            private AdjacencyCursor cursor;
            private int degree;
            private long target = AdjacencyCursor.NOT_FOUND;
            private int localPosition;

            void reset(AdjacencyCursor cursor, int degree) {
                this.cursor = cursor;
                this.degree = degree;
                this.target = AdjacencyCursor.NOT_FOUND;
            }

            boolean next() {
                if (!cursor.hasNextVLong()) {
                    target = AdjacencyCursor.NOT_FOUND;
                    return false;
                }

                // Mark the first entry of a parallel-edge run, matching the slot read during aggregation.
                // Duplicates still consume positions even though they must not create extra triangles.
                localPosition = degree - cursor.remaining();
                target = cursor.nextVLong();
                while (cursor.hasNextVLong() && cursor.peekVLong() == target) {
                    cursor.nextVLong();
                }
                return true;
            }

            void advanceTo(long minimumTarget) {
                while (target != AdjacencyCursor.NOT_FOUND && target < minimumTarget) {
                    next();
                }
            }
        }

    }

    // Each source belongs to one task. Batch its first 65,536 adjacency positions to limit repeated
    // atomic updates while bounding per-task memory. Later positions and the third edge of each
    // triangle are marked directly. Each task owns its local batch, but all tasks share triangleEdges.
    // Flushing uses atomic bit updates because other tasks can mark the same edge or other bits
    // in the same bitmap word.
    private static final class SourceEdgeBatch {
        private final long[] words = new long[SOURCE_EDGE_BATCH_WORDS];
        // Sparse sources should not scan and clear all 1,024 words on every flush.
        private final int[] dirtyWordIndexes = new int[SOURCE_EDGE_BATCH_WORDS];
        private int dirtyWordCount;

        void mark(long localPosition, long sourceOffset, HugeAtomicBitSet triangleEdges) {
            if (localPosition >= SOURCE_EDGE_BATCH_BITS) {
                triangleEdges.set(sourceOffset + localPosition);
                return;
            }

            int wordIndex = (int) (localPosition / Long.SIZE);
            long word = words[wordIndex];
            if (word == 0) {
                dirtyWordIndexes[dirtyWordCount++] = wordIndex;
            }
            words[wordIndex] = word | (1L << (localPosition % Long.SIZE));
        }

        void flush(HugeAtomicBitSet triangleEdges, long sourceOffset) {
            for (int dirtyIndex = 0; dirtyIndex < dirtyWordCount; dirtyIndex++) {
                int wordIndex = dirtyWordIndexes[dirtyIndex];
                long word = words[wordIndex];
                while (word != 0) {
                    int bitIndex = Long.numberOfTrailingZeros(word);
                    triangleEdges.set(sourceOffset + (long) wordIndex * Long.SIZE + bitIndex);
                    word &= word - 1;
                }
                words[wordIndex] = 0;
            }
            dirtyWordCount = 0;
        }
    }

    /**
     * Aggregates neighbor counts and triangle-count sums after triangle discovery finishes.
     * Each undirected pair is visited once at its higher-ID endpoint; the edge bitmap
     * determines whether it also contributes to the triangle-neighbor totals.
     * Source contributions are batched locally, but all shared-array updates remain
     * atomic because other tasks can contribute to either endpoint.
     */
    private class SumTask implements Runnable {
        private final HugeAtomicLongArray triangleNeighbors;
        private final HugeAtomicLongArray allNeighbors;
        private final HugeAtomicLongArray triangleNeighborsTriangleSums;
        private final HugeAtomicLongArray allNeighborsTriangleSums;
        private AdjacencyEntryIndex.LowerNeighborCursor lowerNeighbors;

        SumTask(
            HugeAtomicLongArray triangleNeighbors,
            HugeAtomicLongArray allNeighbors,
            HugeAtomicLongArray triangleNeighborsTriangleSums,
            HugeAtomicLongArray allNeighborsTriangleSums
        ) {
            this.triangleNeighbors = triangleNeighbors;
            this.allNeighbors = allNeighbors;
            this.triangleNeighborsTriangleSums = triangleNeighborsTriangleSums;
            this.allNeighborsTriangleSums = allNeighborsTriangleSums;
        }

        @Override
        public void run() {
            long nodeCount = graph.nodeCount();
            boolean filterByDegree = maxDegree < Long.MAX_VALUE;
            long batchStart;
            while ((batchStart = nextNodeId.getAndAdd(SUM_TASK_BATCH_SIZE)) < nodeCount && terminationFlag.running()) {
                long batchEnd = Math.min(batchStart + SUM_TASK_BATCH_SIZE, nodeCount);
                for (long node = batchStart; node < batchEnd; node++) {
                    if (filterByDegree && adjacencyEntryIndex.degreeForIntersection(node) > maxDegree) {
                        progressTracker.onProgress();
                        continue;
                    }
                    long nodeTriangleCount = triangleCounts.get(node);
                    long sourceTriangleNeighbors = 0;
                    long sourceAllNeighbors = 0;
                    long sourceTriangleNeighborsTriangleSum = 0;
                    long sourceAllNeighborsTriangleSum = 0;
                    lowerNeighbors = adjacencyEntryIndex.cursorForNode(lowerNeighbors, node);
                    while (lowerNeighbors.hasNext()) {
                        long neighbor = lowerNeighbors.target();
                        long neighborTriangleCount = triangleCounts.get(neighbor);

                        // The lower-ID restriction visits each undirected pair once, so update both endpoints.
                        // Batch this source's contributions; neighbor contributions remain shared across tasks.
                        sourceAllNeighbors++;
                        allNeighbors.getAndAdd(neighbor, 1);
                        sourceAllNeighborsTriangleSum += neighborTriangleCount;
                        allNeighborsTriangleSums.getAndAdd(neighbor, nodeTriangleCount);

                        if (triangleEdges.get(lowerNeighbors.globalPosition())) {
                            sourceTriangleNeighbors++;
                            triangleNeighbors.getAndAdd(neighbor, 1);
                            sourceTriangleNeighborsTriangleSum += neighborTriangleCount;
                            triangleNeighborsTriangleSums.getAndAdd(neighbor, nodeTriangleCount);
                        }
                        lowerNeighbors.next();
                    }
                    // Other sources can contribute to this node concurrently, so flushing still requires additions.
                    if (sourceTriangleNeighbors != 0) {
                        triangleNeighbors.getAndAdd(node, sourceTriangleNeighbors);
                        triangleNeighborsTriangleSums.getAndAdd(node, sourceTriangleNeighborsTriangleSum);
                    }
                    if (sourceAllNeighbors != 0) {
                        allNeighbors.getAndAdd(node, sourceAllNeighbors);
                        allNeighborsTriangleSums.getAndAdd(node, sourceAllNeighborsTriangleSum);
                    }
                    progressTracker.onProgress();
                }
            }
        }
    }
}
