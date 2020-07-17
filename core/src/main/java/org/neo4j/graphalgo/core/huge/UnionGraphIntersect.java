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

public class UnionGraphIntersect extends GraphIntersect<CompositeAdjacencyCursor> {

    private final CompositeAdjacencyList compositeAdjacencyList;
    private final CompositeAdjacencyOffsets compositeAdjacencyOffsets;

    UnionGraphIntersect(
        CompositeAdjacencyList compositeAdjacencyList,
        CompositeAdjacencyOffsets compositeAdjacencyOffsets,
        long maxDegree
    ) {
        super(
            compositeAdjacencyList.rawDecompressingCursor(),
            compositeAdjacencyList.rawDecompressingCursor(),
            compositeAdjacencyList.rawDecompressingCursor(),
            compositeAdjacencyList.rawDecompressingCursor(),
            maxDegree
        );
        this.compositeAdjacencyList = compositeAdjacencyList;
        this.compositeAdjacencyOffsets = compositeAdjacencyOffsets;
    }

    @Override
    long skipUntil(CompositeAdjacencyCursor cursor, long nodeId) {
        return cursor.skipUntil(nodeId);
    }

    @Override
    long advance(CompositeAdjacencyCursor cursor, long nodeId) {
        return 0;
    }

    @Override
    void copyFrom(
        CompositeAdjacencyCursor sourceCursor, CompositeAdjacencyCursor targetCursor
    ) {

    }

    @Override
    CompositeAdjacencyCursor cursor(long node, CompositeAdjacencyCursor reuse) {
        var innerCursors = reuse.cursors();

        var innerOffsets = compositeAdjacencyOffsets.adjacencyOffsets();

        for (int i = 0; i < innerOffsets.size(); i++) {
            long offset = innerOffsets.get(i).get(node);
            if (offset == 0L) {
                innerCursors.set(i, empty.cursors().get(i));
            } else {
                TransientAdjacencyList.decompressingCursor(
                    (TransientAdjacencyList.DecompressingCursor) innerCursors.get(i), offset);
            }
        }
        return reuse;
    }

    @Override
    int degree(long node) {
        return 0;
    }
}
