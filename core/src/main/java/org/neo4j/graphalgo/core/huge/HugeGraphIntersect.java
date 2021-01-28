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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.api.AdjacencyOffsets;

public class HugeGraphIntersect extends GraphIntersect<TransientAdjacencyList.DecompressingCursor> {

    private final TransientAdjacencyList adjacency;
    private final AdjacencyOffsets offsets;

    HugeGraphIntersect(final TransientAdjacencyList adjacency, final AdjacencyOffsets offsets, long maxDegree) {
        super(
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            adjacency.rawDecompressingCursor(),
            maxDegree
        );
        this.adjacency = adjacency;
        this.offsets = offsets;
    }

    @Override
    protected long skipUntil(TransientAdjacencyList.DecompressingCursor cursor, long nodeId) {
        return cursor.skipUntil(nodeId);
    }

    @Override
    protected long advance(TransientAdjacencyList.DecompressingCursor cursor, long nodeId) {
        return cursor.advance(nodeId);
    }

    @Override
    protected void copyFrom(
        TransientAdjacencyList.DecompressingCursor sourceCursor, TransientAdjacencyList.DecompressingCursor targetCursor
    ) {
        targetCursor.copyFrom(sourceCursor);
    }

    @Override
    public TransientAdjacencyList.DecompressingCursor cursor(
        long node,
        TransientAdjacencyList.DecompressingCursor reuse
    ) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return TransientAdjacencyList.decompressingCursor(reuse, offset);
    }

    @Override
    protected int degree(long node) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return adjacency.degree(offset);
    }
}
