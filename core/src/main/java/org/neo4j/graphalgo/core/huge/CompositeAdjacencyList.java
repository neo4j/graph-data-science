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
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.PropertyCursor;
import org.neo4j.graphalgo.core.compress.CompressedTopology;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CompositeAdjacencyList implements AdjacencyList {

    private final List<CompressedTopology> adjacencies;

    CompositeAdjacencyList(List<CompressedTopology> adjacencies) {
        this.adjacencies = adjacencies;
    }

    public int size() {
        return adjacencies.size();
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
        List<AdjacencyCursor> adjacencyCursors = adjacencies
            .stream()
            .map(adj -> adj.adjacencyList().rawDecompressingCursor())
            .collect(Collectors.toList());
        return new CompositeAdjacencyCursor(adjacencyCursors);
    }

    @Override
    public CompositeAdjacencyCursor decompressingCursor(long nodeId, int unusedDegree) {
        var adjacencyCursors = new ArrayList<AdjacencyCursor>(adjacencies.size());
        forEachOffset(nodeId, (adjacencyList, offset, degree) -> {
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
        adjacencies.forEach(CompressedTopology::close);
    }

    public void forEachOffset(long nodeId, CompositeIndexedOffsetOperator func) {
        for (var adjacency : adjacencies) {
            long offset = adjacency.adjacencyOffsets().get(nodeId);
            int degree = adjacency.adjacencyDegrees().degree(nodeId);
            func.apply(adjacency.adjacencyList(), offset, degree);
        }
    }

    @FunctionalInterface
    public interface CompositeIndexedOffsetOperator {
        void apply(AdjacencyList list, long offset, int degree);
    }
}
