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
    public void intersectAll(long nodeA, IntersectionConsumer consumer) {
        // check the first node's degree
        if (!degreeFilter.test(nodeA)) {
            return;
        }

        AdjacencyOffsets offsets = this.offsets;
        AdjacencyList adjacency = this.adjacency;
        AdjacencyList.DecompressingCursor neighboursAMain = cursor(nodeA, cache, offsets, adjacency);

        // find first neighbour B of A with id > A
        long nodeB = neighboursAMain.skipUntil(nodeA);
        // if there is no such neighbour -> no triangle (or we already found it)
        if (nodeA > nodeB) {
            return;
        }

        // iterates over neighbours of A
        AdjacencyList.DecompressingCursor neighboursA = cacheA;
        // current neighbour of A
        long nodeCa;
        // iterates over neighbours of B
        AdjacencyList.DecompressingCursor neighboursB = cacheB;
        // current neighbour of B
        long nodeCb;

        // last node where Ca = Cb
        // prevents counting a new triangle for parallel relationships
        long triangleC;

        // for all neighbours of A
        while (neighboursAMain.hasNextVLong()) {
            // we have not yet seen a triangle
            triangleC = -1;
            // check the second node's degree
            if (degreeFilter.test(nodeB)) {
                neighboursB = cursor(nodeB, neighboursB, offsets, adjacency);
                // find first neighbour Cb of B with id > B
                nodeCb = neighboursB.skipUntil(nodeB);

                // check the third node's degree
                if (nodeCb > nodeB && degreeFilter.test(nodeCb)) {
                    // copy the state of A's cursor
                    neighboursA.copyFrom(neighboursAMain);
                    // find the first neighbour Ca of A with id >= Cb
                    nodeCa = neighboursA.advance(nodeCb);

                    // if Ca = Cb we have found a triangle
                    // we only submit one triangle per parallel relationship
                    if (nodeCa == nodeCb && nodeCa > triangleC) {
                        consumer.accept(nodeA, nodeB, nodeCa);
                        triangleC = nodeCa;
                    }

                    // while both A and B have more neighbours
                    while (neighboursB.hasNextVLong() && neighboursA.hasNextVLong()) {
                        // take the next neighbour Cb of B
                        nodeCb = neighboursB.nextVLong();
                        if (nodeCb > nodeCa) {
                            // if Cb > Ca, take the next neighbour Ca of A with id >= Cb
                            nodeCa = neighboursA.advance(nodeCb);
                        }
                        // check for triangle
                        if (nodeCa == nodeCb && nodeCa > triangleC && degreeFilter.test(nodeCa)) {
                            consumer.accept(nodeA, nodeB, nodeCa);
                            triangleC = nodeCa;
                        }
                    }

                    // it is possible that the last Ca > Cb, but there are no more neighbours Ca of A
                    // so if there are more neighbours Cb of B
                    if (neighboursB.hasNextVLong()) {
                        // we take the next neighbour Cb of B with id >= Ca
                        nodeCb = neighboursB.advance(nodeCa);
                        // check for triangle
                        if (nodeCa == nodeCb && nodeCa > triangleC && degreeFilter.test(nodeCa)) {
                            consumer.accept(nodeA, nodeB, nodeCa);
                        }
                    }
                }
            }

            // skip until the next neighbour B of A with id > (current) B
            nodeB = skipUntil(neighboursAMain, nodeB);
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
