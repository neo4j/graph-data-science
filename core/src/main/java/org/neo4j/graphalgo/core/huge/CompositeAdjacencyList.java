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
import org.neo4j.graphalgo.api.AdjacencyDegrees;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.api.PropertyCursor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CompositeAdjacencyList implements AdjacencyList {

    private final List<AdjacencyDegrees> adjacencyDegrees;
    private final List<AdjacencyList> adjacencyLists;
    private final List<AdjacencyOffsets> adjacencyOffsets;

    CompositeAdjacencyList(
        List<AdjacencyDegrees> adjacencyDegrees,
        List<AdjacencyList> adjacencyLists,
        List<AdjacencyOffsets> adjacencyOffsets
    ) {
        this.adjacencyDegrees = adjacencyDegrees;
        this.adjacencyLists = adjacencyLists;
        this.adjacencyOffsets = adjacencyOffsets;
    }

    public List<AdjacencyList> adjacencyLists() {
        return adjacencyLists;
    }

    @Override
    public PropertyCursor rawCursor() {
        throw new UnsupportedOperationException("CompositeAdjacencyList#rawCursor is not supported");
    }

    @Override
    public PropertyCursor cursor(long offset, int degree) {
        throw new UnsupportedOperationException("CompositeAdjacencyList#cursor is not supported");
    }

    @Override
    public CompositeAdjacencyCursor rawDecompressingCursor() {
        List<AdjacencyCursor> adjacencyCursors = adjacencyLists
            .stream()
            .map(AdjacencyList::rawDecompressingCursor)
            .collect(Collectors.toList());
        return new CompositeAdjacencyCursor(adjacencyCursors);
    }

    @Override
    public CompositeAdjacencyCursor decompressingCursor(long nodeId, int unusedDegree) {
        var adjacencyCursors = new ArrayList<AdjacencyCursor>(adjacencyLists.size());
        forEachOffset(nodeId, (adjacencyList, index, offset, degree, hasAdjacency) -> {
            var cursor = adjacencyList.rawDecompressingCursor();
            if (offset != 0) {
                cursor.init(offset, degree);
            }
            adjacencyCursors.add(cursor);
        });
        return new CompositeAdjacencyCursor(adjacencyCursors);
    }

    @Override
    public void close() {
        adjacencyLists.forEach(AdjacencyList::close);
    }

    public void forEachOffset(long nodeId, CompositeIndexedOffsetOperator func) {
        for (int i = 0; i < adjacencyLists.size(); i++) {
            long offset = adjacencyOffsets.get(i).get(nodeId);
            int degree = adjacencyDegrees.get(i).degree(nodeId);
            func.apply(adjacencyLists.get(i), i, offset, degree, offset != 0);
        }
    }

    @FunctionalInterface
    public interface CompositeIndexedOffsetOperator {
        void apply(AdjacencyList list, int index, long offset, int degree, boolean hasAdjacency);
    }
}
