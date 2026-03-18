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

import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.function.LongToIntFunction;

/**
 * Assigns positions to graph adjacency entries for Triangle Centrality's triangle-neighbor bitset
 * and provides lower-ID neighbor cursors.
 *
 * The graph adjacency API does not expose a portable, zero-based offset for each node's entries.
 * This class builds a size {@code n + 1} prefix-sum array of node degrees so a cursor can map an
 * entry to {@code offsets[source] + rawPosition}. The offsets include all entries in the selected
 * graph view, including those skipped by the lower-neighbor cursor.
 * Each undirected edge is marked at its higher-ID endpoint, using the first entry for parallel
 * relationships. Self-loops and higher-ID neighbors retain their slots but are never yielded.
 * Keeping those unused slots lets both intersection and aggregation locate the same bit from
 * the existing adjacency order, without copying targets or building a deduplicated edge index.
 */
final class AdjacencyEntryIndex {

    private final Graph graph;
    private final long maxDegree;
    private final boolean filterByDegree;
    private final LongToIntFunction degreeFunction;
    private final TriangleCentralityAdjacencyCursorFactory cursorFactory;
    private final HugeLongArray offsets;

    private AdjacencyEntryIndex(
        Graph graph,
        long maxDegree,
        LongToIntFunction degreeFunction,
        TriangleCentralityAdjacencyCursorFactory cursorFactory,
        HugeLongArray offsets
    ) {
        this.graph = graph;
        this.maxDegree = maxDegree;
        this.filterByDegree = maxDegree < Long.MAX_VALUE;
        this.degreeFunction = degreeFunction;
        this.cursorFactory = cursorFactory;
        this.offsets = offsets;
    }

    static AdjacencyEntryIndex create(
        Graph graph,
        long maxDegree,
        Concurrency concurrency,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        var cursorFactory = TriangleCentralityAdjacencyCursorFactory.forGraph(graph);
        var degreeFunction = intersectionDegreeFunction(graph);
        long nodeCount = graph.nodeCount();
        var offsets = HugeLongArray.newArray(nodeCount + 1);

        ParallelUtil.parallelForEachNode(nodeCount, concurrency, terminationFlag, node -> {
            offsets.set(node + 1, graph.degree(node));
        });

        for (long node = 1; node <= nodeCount; node++) {
            terminationFlag.assertRunning();
            offsets.set(node, offsets.get(node) + offsets.get(node - 1));
            progressTracker.onProgress();
        }

        return new AdjacencyEntryIndex(graph, maxDegree, degreeFunction, cursorFactory, offsets);
    }

    private static LongToIntFunction intersectionDegreeFunction(Graph graph) {
        if (graph instanceof NodeFilteredGraph nodeFilteredGraph) {
            // Preserve Triangle Count's behavior: filtering out a hub's neighbors must not make
            // that hub eligible for intersections. Result exclusion still uses the selected degree.
            var rootGraph = nodeFilteredGraph.graph();
            return node -> rootGraph.degree(nodeFilteredGraph.toRootNodeId(node));
        }
        return graph::degree;
    }

    long adjacencyEntryCount() {
        return offsets.get(graph.nodeCount());
    }

    long offset(long node) {
        return offsets.get(node);
    }

    int degreeForIntersection(long node) {
        return degreeFunction.applyAsInt(node);
    }

    boolean supportsRawPositionTracking() {
        return graph instanceof HugeGraph;
    }

    AdjacencyCursor rawCursorForNode(AdjacencyCursor reuse, long node) {
        return cursorFactory.cursorForNode(reuse, node);
    }

    LowerNeighborCursor cursorForNode(LowerNeighborCursor reuse, long node) {
        var cursor = cursorFactory.cursorForNode(reuse == null ? null : reuse.cursor, node);
        if (reuse == null) {
            return new LowerNeighborCursor(
                cursor,
                degreeFunction,
                node,
                maxDegree,
                filterByDegree,
                offsets.get(node)
            );
        }
        return reuse.reset(cursor, node, offsets.get(node));
    }

    static final class LowerNeighborCursor {
        private AdjacencyCursor cursor;
        private final LongToIntFunction degreeFunction;
        private final long maxDegree;
        private final boolean filterByDegree;
        private long source;
        private long sourceOffset;
        private long previousTarget = AdjacencyCursor.NOT_FOUND;
        private long nextTarget = AdjacencyCursor.NOT_FOUND;
        private long nextGlobalPosition = AdjacencyCursor.NOT_FOUND;
        private long consumedEntries;
        private boolean canUseCursorAdvance = true;

        private LowerNeighborCursor(
            AdjacencyCursor cursor,
            LongToIntFunction degreeFunction,
            long source,
            long maxDegree,
            boolean filterByDegree,
            long sourceOffset
        ) {
            this.cursor = cursor;
            this.degreeFunction = degreeFunction;
            this.maxDegree = maxDegree;
            this.filterByDegree = filterByDegree;
            reset(cursor, source, sourceOffset);
        }

        private LowerNeighborCursor reset(AdjacencyCursor cursor, long source, long sourceOffset) {
            this.cursor = cursor;
            this.source = source;
            this.sourceOffset = sourceOffset;
            this.previousTarget = AdjacencyCursor.NOT_FOUND;
            this.nextTarget = AdjacencyCursor.NOT_FOUND;
            this.nextGlobalPosition = AdjacencyCursor.NOT_FOUND;
            this.consumedEntries = 0;
            advance();
            return this;
        }

        boolean hasNext() {
            return nextTarget != AdjacencyCursor.NOT_FOUND;
        }

        long target() {
            return nextTarget;
        }

        long globalPosition() {
            return nextGlobalPosition;
        }

        void next() {
            advance();
        }

        void advanceTo(long target) {
            if (!canUseCursorAdvance) {
                advanceSeriallyTo(target);
                return;
            }

            try {
                int remainingBeforeAdvance = cursor.remaining();
                long advancedTarget = cursor.advance(target);
                // A seek can consume duplicates and excluded targets; all still occupy bitmap slots.
                consumedEntries += remainingBeforeAdvance - cursor.remaining();

                // The adjacency list is sorted, so no later target can be a lower neighbor.
                if (advancedTarget == AdjacencyCursor.NOT_FOUND || advancedTarget >= source) {
                    nextTarget = AdjacencyCursor.NOT_FOUND;
                    nextGlobalPosition = AdjacencyCursor.NOT_FOUND;
                    return;
                }

                previousTarget = advancedTarget;
                if (eligible(advancedTarget)) {
                    nextTarget = advancedTarget;
                    nextGlobalPosition = sourceOffset + consumedEntries - 1;
                    return;
                }

                advance();
            } catch (UnsupportedOperationException ignored) {
                // Filtered cursors may not expose remaining(); serial traversal preserves entry positions.
                canUseCursorAdvance = false;
                advanceSeriallyTo(target);
            }
        }

        private void advanceSeriallyTo(long target) {
            advance();
            while (hasNext() && nextTarget < target) {
                advance();
            }
        }

        private void advance() {
            nextTarget = AdjacencyCursor.NOT_FOUND;
            nextGlobalPosition = AdjacencyCursor.NOT_FOUND;

            while (cursor.hasNextVLong()) {
                long target = cursor.nextVLong();
                long rawPosition = consumedEntries++;
                // The adjacency list is sorted, so no later target can be a lower neighbor.
                if (target >= source) {
                    return;
                }
                if (target == previousTarget) {
                    continue;
                }
                previousTarget = target;
                if (eligible(target)) {
                    nextTarget = target;
                    nextGlobalPosition = sourceOffset + rawPosition;
                    return;
                }
            }
        }

        private boolean eligible(long target) {
            return target < source && (!filterByDegree || degreeFunction.applyAsInt(target) <= maxDegree);
        }
    }

}
