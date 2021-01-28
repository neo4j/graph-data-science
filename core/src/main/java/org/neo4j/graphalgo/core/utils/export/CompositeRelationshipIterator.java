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
package org.neo4j.graphalgo.core.utils.export;

import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.ObjectObjectMap;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyList;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.util.Map;

class CompositeRelationshipIterator {

    private final TransientAdjacencyList adjacencyList;
    private final TransientAdjacencyOffsets adjacencyOffsets;
    private final Map<String, TransientAdjacencyList> propertyLists;
    private final ObjectObjectMap<String, TransientAdjacencyOffsets> propertyOffsets;

    private final String[] propertyKeys;
    private final TransientAdjacencyList.DecompressingCursor cursorCache;
    private final ObjectObjectMap<String, TransientAdjacencyList.Cursor> propertyCursorCache;

    CompositeRelationshipIterator(
        TransientAdjacencyList adjacencyList,
        TransientAdjacencyOffsets adjacencyOffsets,
        Map<String, TransientAdjacencyList> propertyLists,
        Map<String, TransientAdjacencyOffsets> propertyOffsets
    ) {
        this(adjacencyList, adjacencyOffsets, propertyLists, hppcCopy(propertyOffsets));
    }

    private CompositeRelationshipIterator(
        TransientAdjacencyList adjacencyList,
        TransientAdjacencyOffsets adjacencyOffsets,
        Map<String, TransientAdjacencyList> propertyLists,
        ObjectObjectMap<String, TransientAdjacencyOffsets> propertyOffsets
    ) {
        this.adjacencyList = adjacencyList;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyLists = propertyLists;
        this.propertyOffsets = propertyOffsets;

        // create data structures for internal use
        this.propertyKeys = propertyLists.keySet().toArray(new String[0]);
        this.cursorCache = adjacencyList.rawDecompressingCursor();
        this.propertyCursorCache = new ObjectObjectHashMap<>(propertyLists.size());
        this.propertyLists.forEach((key, list) -> this.propertyCursorCache.put(key, list.rawCursor()));
    }

    private static ObjectObjectMap<String, TransientAdjacencyOffsets> hppcCopy(Map<String, TransientAdjacencyOffsets> offsets) {
        var map = new ObjectObjectHashMap<String, TransientAdjacencyOffsets>(offsets.size());
        offsets.forEach(map::put);
        return map;
    }

    CompositeRelationshipIterator concurrentCopy() {
        return new CompositeRelationshipIterator(adjacencyList, adjacencyOffsets, propertyLists, propertyOffsets);
    }

    int propertyCount() {
        return propertyKeys.length;
    }

    void forEachRelationship(long sourceId, String relType, InputEntityVisitor visitor) throws IOException {
        var offset = adjacencyOffsets.get(sourceId);

        if (offset == 0L) {
            return;
        }

        // init adjacency cursor
        var adjacencyCursor = TransientAdjacencyList.decompressingCursor(cursorCache, offset);
        // init property cursors
        for (var propertyKey : propertyKeys) {
            propertyCursorCache.put(
                propertyKey,
                TransientAdjacencyList.cursor(
                    propertyCursorCache.get(propertyKey),
                    propertyOffsets.get(propertyKey).get(sourceId)
                )
            );
        }

        // in-step iteration of adjacency and property cursors
        while (adjacencyCursor.hasNextVLong()) {
            visitor.startId(sourceId);
            visitor.endId(adjacencyCursor.nextVLong());
            visitor.type(relType);

            propertyCursorCache.forEach((ObjectObjectProcedure<String, TransientAdjacencyList.Cursor>) (propertyKey, propertyCursor) -> {
                visitor.property(
                    propertyKey,
                    Double.longBitsToDouble(propertyCursor.nextLong())
                );
            });

            visitor.endOfEntity();
        }
    }
}
