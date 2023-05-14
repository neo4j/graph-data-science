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
package org.neo4j.gds.core.huge;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ImmutableMemoryInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public final class CompositeAdjacencyList implements AdjacencyList {

    private final List<AdjacencyList> adjacencyLists;
    private final CompositeAdjacencyCursorFactory compositeAdjacencyCursorFactory;
    private final AdjacencyCursorWrapperFactory adjacencyCursorWrapperFactory;

    @FunctionalInterface
    interface CompositeAdjacencyCursorFactory {
        CompositeAdjacencyCursor create(List<AdjacencyCursor> cursors);
    }

    @FunctionalInterface
    interface AdjacencyCursorWrapperFactory {
        AdjacencyCursor create(AdjacencyCursor adjacencyCursor);

        class Identity implements AdjacencyCursorWrapperFactory {
            @Override
            public AdjacencyCursor create(AdjacencyCursor adjacencyCursor) {
                return adjacencyCursor;
            }
        }
    }

    static CompositeAdjacencyList of(List<AdjacencyList> adjacencyLists) {
        return new CompositeAdjacencyList(
            adjacencyLists,
            CompositeAdjacencyCursor::new,
            new AdjacencyCursorWrapperFactory.Identity()
        );
    }

    static CompositeAdjacencyList withFilteredIdMap(List<AdjacencyList> adjacencyLists, IdMap filteredIdMap) {
        assert filteredIdMap instanceof NodeFilteredGraph;
        var adjacencyCursorWrapperFactory = (AdjacencyCursorWrapperFactory) cursor -> new NodeFilteredAdjacencyCursor(
            cursor,
            filteredIdMap
        );
        var compositeAdjacencyCursorFactory = (CompositeAdjacencyCursorFactory) cursors -> {
            List<AdjacencyCursor> wrappedCursors = cursors
                .stream()
                .map(adjacencyCursorWrapperFactory::create)
                .collect(Collectors.toList());
            return new CompositeAdjacencyCursor(wrappedCursors);
        };
        return new CompositeAdjacencyList(
            adjacencyLists,
            compositeAdjacencyCursorFactory,
            adjacencyCursorWrapperFactory
        );
    }

    private CompositeAdjacencyList(
        List<AdjacencyList> adjacencyLists,
        CompositeAdjacencyCursorFactory compositeAdjacencyCursorFactory,
        AdjacencyCursorWrapperFactory adjacencyCursorWrapperFactory
    ) {
        this.adjacencyLists = adjacencyLists;
        this.compositeAdjacencyCursorFactory = compositeAdjacencyCursorFactory;
        this.adjacencyCursorWrapperFactory = adjacencyCursorWrapperFactory;
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
        return compositeAdjacencyCursorFactory.create(cursors);
    }

    @Override
    public CompositeAdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node) {
        return adjacencyCursor(reuse, node, Double.NaN);
    }

    @Override
    public CompositeAdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        if (reuse instanceof CompositeAdjacencyCursor) {
            var compositeReuse = (CompositeAdjacencyCursor) reuse;
            var iter = compositeReuse.cursors().listIterator();
            while (iter.hasNext()) {
                var index = iter.nextIndex();
                var cursor = iter.next();
                var newCursor = adjacencyLists.get(index).adjacencyCursor(cursor, node, fallbackValue);
                if (newCursor != cursor) {
                    iter.set(adjacencyCursorWrapperFactory.create(newCursor));
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
        return compositeAdjacencyCursorFactory.create(cursors);
    }

    @Override
    public MemoryInfo memoryInfo() {
        var memoryInfo = this.adjacencyLists.stream().map(AdjacencyList::memoryInfo).collect(Collectors.toList());

        var pages = memoryInfo
            .stream()
            .mapToLong(MemoryInfo::pages)
            .sum();

        var bytesOnHeap = memoryInfo
            .stream()
            .map(MemoryInfo::bytesOnHeap)
            .filter(OptionalLong::isPresent)
            .mapToLong(OptionalLong::getAsLong)
            .reduce(Long::sum);

        var bytesOffHeap = memoryInfo
            .stream()
            .map(MemoryInfo::bytesOffHeap)
            .filter(OptionalLong::isPresent)
            .mapToLong(OptionalLong::getAsLong)
            .reduce(Long::sum);

        return ImmutableMemoryInfo.builder().pages(pages).bytesOnHeap(bytesOnHeap).bytesOffHeap(bytesOffHeap).build();
    }

}
