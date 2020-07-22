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

import java.util.ArrayList;

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
        return cursor.advance(nodeId);
    }

    @Override
    void copyFrom(
        CompositeAdjacencyCursor sourceCursor, CompositeAdjacencyCursor targetCursor
    ) {
        targetCursor.copyFrom(sourceCursor);
    }

    @Override
    CompositeAdjacencyCursor cursor(long nodeId, CompositeAdjacencyCursor reuse) {
        var adjacencyLists = compositeAdjacencyList.adjacencyLists();
        var adjacencyOffsets = compositeAdjacencyOffsets.adjacencyOffsets();
        var adjacencyCursors = new ArrayList<AdjacencyCursor>(adjacencyLists.size());
        var cursors = reuse.cursors();
        var emptyCursors = empty.cursors();

        for (int i = 0; i < adjacencyLists.size(); i++) {
            var offset = adjacencyOffsets.get(i).get(nodeId);
            if (offset == 0) {
                adjacencyCursors.add(i, emptyCursors.get(i));
            } else {
                adjacencyCursors.add(i, TransientAdjacencyList.decompressingCursor((TransientAdjacencyList.DecompressingCursor) cursors.get(i), offset));
            }
        }
        return new CompositeAdjacencyCursor(adjacencyCursors);
    }

    @Override
    int degree(long node) {
        return 0; //TODO: implement
    }
}
