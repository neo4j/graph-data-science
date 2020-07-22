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

import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;

import java.util.function.LongPredicate;

/**
 * An instance of this is not thread-safe; Iteration/Intersection on multiple threads will
 * throw misleading {@link NullPointerException}s.
 * Instances are however safe to use concurrently with other {@link org.neo4j.graphalgo.api.RelationshipIterator}s.
 */

class HugeGraphIntersectImpl implements RelationshipIntersect {

    private TransientAdjacencyList adjacency;
    private TransientAdjacencyOffsets offsets;
    private TransientAdjacencyList.DecompressingCursor empty;
    private TransientAdjacencyList.DecompressingCursor cache;
    private TransientAdjacencyList.DecompressingCursor cacheA;
    private TransientAdjacencyList.DecompressingCursor cacheB;
    private final LongPredicate degreeFilter;

    HugeGraphIntersectImpl(final TransientAdjacencyList adjacency, final TransientAdjacencyOffsets offsets, long maxDegree) {
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
        if(!degreeFilter.test(nodeIdA)) {
            return;
        }

        TransientAdjacencyOffsets offsets = this.offsets;
        TransientAdjacencyList adjacency = this.adjacency;

        TransientAdjacencyList.DecompressingCursor mainDecompressingCursor = cursor(nodeIdA, cache, offsets, adjacency);
        long nodeIdB = mainDecompressingCursor.skipUntil(nodeIdA);
        if (nodeIdB <= nodeIdA) {
            return;
        }

        TransientAdjacencyList.DecompressingCursor lead, follow, decompressingCursorA = cacheA, decompressingCursorB = cacheB;
        long nodeIdC, currentA, s, t;

        long lastNodeB;
        long lastNodeC;
        while (mainDecompressingCursor.hasNextVLong()) {
            lastNodeC = -1;
            if (degreeFilter.test(nodeIdB)) {
                decompressingCursorB = cursor(nodeIdB, decompressingCursorB, offsets, adjacency);
                nodeIdC = decompressingCursorB.skipUntil(nodeIdB);
                if (nodeIdC > nodeIdB && degreeFilter.test(nodeIdC)) {
                    decompressingCursorA.copyFrom(mainDecompressingCursor);
                    currentA = decompressingCursorA.advance(nodeIdC);

                    if (currentA == nodeIdC && nodeIdC > lastNodeC) {
                        consumer.accept(nodeIdA, nodeIdB, nodeIdC);
                        lastNodeC = nodeIdC;
                    }

                    if (decompressingCursorA.remaining() <= decompressingCursorB.remaining()) {
                        lead = decompressingCursorA;
                        follow = decompressingCursorB;
                    } else {
                        lead = decompressingCursorB;
                        follow = decompressingCursorA;
                    }

                    while (lead.hasNextVLong() && follow.hasNextVLong()) {
                        s = lead.nextVLong();
                        t = follow.advance(s);
                        if (t == s && t > lastNodeC) {
                            consumer.accept(nodeIdA, nodeIdB, s);
                            lastNodeC = t;
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
        return adjacency.degree(offset);
    }

    private TransientAdjacencyList.DecompressingCursor cursor(
            long node,
            TransientAdjacencyList.DecompressingCursor reuse,
            TransientAdjacencyOffsets offsets,
            TransientAdjacencyList array) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return array.decompressingCursor(reuse, offset);
    }
}
