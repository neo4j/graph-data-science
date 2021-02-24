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

import org.neo4j.graphalgo.api.AdjacencyCursor;

import java.util.ArrayList;

public class UnionGraphIntersect extends GraphIntersect<CompositeAdjacencyCursor> {

    private final CompositeAdjacencyList compositeAdjacencyList;

    public UnionGraphIntersect(
        CompositeAdjacencyList compositeAdjacencyList,
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
    }

    @Override
    protected CompositeAdjacencyCursor cursor(long nodeId, CompositeAdjacencyCursor reuse) {
        var adjacencyCursors = new ArrayList<AdjacencyCursor>(compositeAdjacencyList.adjacencyLists().size());
        var cursors = reuse.cursors();
        var emptyCursors = empty.cursors();

        compositeAdjacencyList.forEachOffset(nodeId, ((index, offset, hasAdjacency) -> adjacencyCursors.add(
            index,
            hasAdjacency
                ? cursors.get(index).initializedTo(offset)
                : emptyCursors.get(index)
        )));
        return new CompositeAdjacencyCursor(adjacencyCursors);
    }

    @Override
    protected int degree(long nodeId) {
        return compositeAdjacencyList.degree(nodeId);
    }
}
