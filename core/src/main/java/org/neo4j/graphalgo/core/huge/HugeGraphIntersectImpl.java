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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;

import java.util.function.LongPredicate;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.graphalgo.api.RelationshipIterator}s.
 */

class HugeGraphIntersectImpl implements RelationshipIntersect {

    private AdjacencyList adjacency;
    private AdjacencyOffsets offsets;
    private AdjacencyList.DecompressingCursor empty;
    private AdjacencyList.DecompressingCursor cache;
    private AdjacencyList.DecompressingCursor cacheA;
    private AdjacencyList.DecompressingCursor cacheB;
    private final LongPredicate degreeFilter;

    HugeGraphIntersectImpl(final AdjacencyList adjacency, final AdjacencyOffsets offsets, long maxDegree) {
        assert adjacency != null;
        assert offsets != null;
        this.adjacency = adjacency;
        this.offsets = offsets;
        cache = adjacency.rawDecompressingCursor();
        cacheA = adjacency.rawDecompressingCursor();
        cacheB = adjacency.rawDecompressingCursor();
        empty = adjacency.rawDecompressingCursor();
        this.degreeFilter = maxDegree < Long.MAX_VALUE
            ? (node) -> degree(node) <= maxDegree
            : (ignore) -> true;
    }

    @Override
    public void intersectAll(long nodeIdA, IntersectionConsumer consumer) {
        // skip high-degree nodes
        if (!degreeFilter.test(nodeIdA)) {
            return;
        }

        AdjacencyOffsets offsets = this.offsets;
        AdjacencyList adjacency = this.adjacency;

        AdjacencyList.DecompressingCursor mainDecompressingCursor = cursor(nodeIdA, cache, offsets, adjacency);
        // find first neighbour B of A id > A
        long nodeIdB = mainDecompressingCursor.skipUntil(nodeIdA);
        if (nodeIdA > nodeIdB) {
            return;
        }

        AdjacencyList.DecompressingCursor cursorToAdvance, decompressingCursorA = cacheA, decompressingCursorB = cacheB;

        long CfromB;
        long CfromA;

        long lastNodeB;
        long lastNodeC;

        while (mainDecompressingCursor.hasNextVLong()) {
            lastNodeC = -1;
            // again, skip high-degree nodes
            if (degreeFilter.test(nodeIdB)) {
                decompressingCursorB = cursor(nodeIdB, decompressingCursorB, offsets, adjacency);
                // find first neighbour C of B with id > B
                CfromB = decompressingCursorB.skipUntil(nodeIdB);
                if (CfromB > nodeIdB && degreeFilter.test(CfromB)) {
                    // copy the state of A's cursor
                    decompressingCursorA.copyFrom(mainDecompressingCursor);
                    // find the first neighbour C' of A with id >= C
                    CfromA = decompressingCursorA.advance(CfromB);

                    // if C' = C we have found a triangle
                    // we only submit one triangle per parallel relationship
                    if (CfromA == CfromB && CfromB > lastNodeC) {
                        consumer.accept(nodeIdA, nodeIdB, CfromB);
                        lastNodeC = CfromB;
                    }

                    // we choose which cursor to advance
                    if (CfromA > CfromB) {
                        cursorToAdvance = decompressingCursorB;
                    } else {
                        // Mainly an optimization .. maybe benchmark ?
                        if (decompressingCursorA.remaining() <= decompressingCursorB.remaining()) {
                            cursorToAdvance = decompressingCursorB;
                        } else {
                            cursorToAdvance = decompressingCursorA;
                        }
                    }

                    // we now advance the chosen cursor while we can
                    while (cursorToAdvance.hasNextVLong()) {
                        // we will switch cursor to advance the one with a lower value
                        if (cursorToAdvance == decompressingCursorB) {
                            // find the next neighbour of B with id >= C'
                            CfromB = cursorToAdvance.advance(CfromA);
                            cursorToAdvance = decompressingCursorA;
                        } else {
                            // find the next neighbour of A with id >= C
                            CfromA = cursorToAdvance.advance(CfromB);
                            cursorToAdvance = decompressingCursorB;
                        }

                        // if C = C' we have found a triangle
                        if (CfromA == CfromB && CfromB > lastNodeC) {
                            consumer.accept(nodeIdA, nodeIdB, CfromB);
                            lastNodeC = CfromB;

                            // Mainly an optimization .. maybe benchmark ?
                            if (decompressingCursorA.remaining() <= decompressingCursorB.remaining()) {
                                cursorToAdvance = decompressingCursorB;
                            } else {
                                cursorToAdvance = decompressingCursorA;
                            }
                        }
                    }
                }
            }

            lastNodeB = nodeIdB;
            while (mainDecompressingCursor.hasNextVLong() && nodeIdB == lastNodeB) {
                nodeIdB = mainDecompressingCursor.nextVLong();
            }
        }
    }

    private int degree(long node) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return adjacency.getDegree(offset);
    }

    private AdjacencyList.DecompressingCursor cursor(
            long node,
            AdjacencyList.DecompressingCursor reuse,
            AdjacencyOffsets offsets,
            AdjacencyList array) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return array.decompressingCursor(reuse, offset);
    }

    private void consumeNodes(
            long startNode,
            AdjacencyList.DecompressingCursor decompressingCursor,
            RelationshipConsumer consumer) {
        //noinspection StatementWithEmptyBody
        while (decompressingCursor.hasNextVLong() && consumer.accept(startNode, decompressingCursor.nextVLong())) ;
    }
}
