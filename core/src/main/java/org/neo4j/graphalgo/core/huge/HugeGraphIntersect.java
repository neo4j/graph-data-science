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

import org.neo4j.graphalgo.api.AdjacencyOffsets;

public class HugeGraphIntersect extends GraphIntersect<TransientAdjacencyList, TransientAdjacencyList.DecompressingCursor> {

    HugeGraphIntersect(final TransientAdjacencyList adjacency, final AdjacencyOffsets offsets, long maxDegree) {
        super(
            adjacency,
            offsets,
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            maxDegree
        );
    }

    @Override
    long skipUntil(TransientAdjacencyList.DecompressingCursor cursor, long nodeId) {
        return cursor.skipUntil(nodeId);
    }

    @Override
    long advance(TransientAdjacencyList.DecompressingCursor cursor, long nodeId) {
        return cursor.advance(nodeId);
    }

    @Override
    void copyFrom(
        TransientAdjacencyList.DecompressingCursor sourceCursor, TransientAdjacencyList.DecompressingCursor targetCursor
    ) {
        targetCursor.copyFrom(sourceCursor);
    }

    @Override
    public TransientAdjacencyList.DecompressingCursor cursor(
        long node,
        TransientAdjacencyList.DecompressingCursor reuse,
        AdjacencyOffsets offsets,
        TransientAdjacencyList array) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return array.decompressingCursor(reuse, offset);
    }

}
