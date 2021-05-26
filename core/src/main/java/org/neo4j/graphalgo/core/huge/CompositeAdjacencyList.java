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

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.PropertyCursor;

import java.util.ArrayList;
import java.util.List;

public class CompositeAdjacencyList implements AdjacencyList {

    private final List<AdjacencyList> adjacencyLists;

    CompositeAdjacencyList(List<AdjacencyList> adjacencyLists) {
        this.adjacencyLists = adjacencyLists;
    }

    public int size() {
        return adjacencyLists.size();
    }

    @Override
    public int degree(long node) {
        long degree = 0;
        for (var adjacency : adjacencyLists) {
            degree += adjacency.degree(node);
        }
        return Math.toIntExact(degree);
    }

    @Override
    public CompositeAdjacencyCursor adjacencyCursor(long node) {
        return adjacencyCursor(node, Double.NaN);
    }

    @Override
    public CompositeAdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        var cursors = new ArrayList<AdjacencyCursor>(adjacencyLists.size());
        for (var adjacency : adjacencyLists) {
            cursors.add(adjacency.adjacencyCursor(node, fallbackValue));
        }
        return new CompositeAdjacencyCursor(cursors);
    }

    @Override
    public CompositeAdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node) {
        return adjacencyCursor(reuse, node, Double.NaN);
    }

    @Override
    public CompositeAdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        if (reuse instanceof CompositeAdjacencyCursor) {
            var compositeReuse = (CompositeAdjacencyCursor) reuse;
            var iter = (compositeReuse).cursors().listIterator();
            while (iter.hasNext()) {
                var index = iter.nextIndex();
                var cursor = iter.next();
                var newCursor = adjacencyLists.get(index).adjacencyCursor(cursor, node, fallbackValue);
                if (newCursor != cursor) {
                    iter.set(newCursor);
                }
            }
            return compositeReuse;
        }
        return adjacencyCursor(node, fallbackValue);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        var cursors = new ArrayList<AdjacencyCursor>(adjacencyLists.size());
        for (var adjacency : adjacencyLists) {
            cursors.add(adjacency.rawAdjacencyCursor());
        }
        return new CompositeAdjacencyCursor(cursors);
    }

    @Override
    public PropertyCursor propertyCursor(long node, double fallbackValue) {
        throw new UnsupportedOperationException("CompositeAdjacencyList#propertyCursor is not supported");
    }

    @Override
    public void close() {
        adjacencyLists.forEach(AdjacencyList::close);
    }
}
