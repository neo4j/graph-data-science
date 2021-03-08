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
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.AdjacencyDegrees;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.api.PropertyCursor;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.util.Map;

class CompositeRelationshipIterator {

    private final AdjacencyDegrees adjacencyDegrees;
    private final AdjacencyList adjacencyList;
    private final AdjacencyOffsets adjacencyOffsets;
    private final Map<String, ? extends AdjacencyList> propertyLists;
    private final ObjectObjectMap<String, AdjacencyOffsets> propertyOffsets;

    private final String[] propertyKeys;
    private final AdjacencyCursor cursorCache;
    private final ObjectObjectMap<String, PropertyCursor> propertyCursorCache;

    CompositeRelationshipIterator(
        AdjacencyDegrees adjacencyDegrees,
        AdjacencyList adjacencyList,
        AdjacencyOffsets adjacencyOffsets,
        Map<String, ? extends AdjacencyList> propertyLists,
        Map<String, ? extends AdjacencyOffsets> propertyOffsets
    ) {
        this(adjacencyDegrees, adjacencyList, adjacencyOffsets, propertyLists, hppcCopy(propertyOffsets));
    }

    private CompositeRelationshipIterator(
        AdjacencyDegrees adjacencyDegrees,
        AdjacencyList adjacencyList,
        AdjacencyOffsets adjacencyOffsets,
        Map<String, ? extends AdjacencyList> propertyLists,
        ObjectObjectMap<String, AdjacencyOffsets> propertyOffsets
    ) {
        this.adjacencyDegrees = adjacencyDegrees;
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

    private static ObjectObjectMap<String, AdjacencyOffsets> hppcCopy(Map<String, ? extends AdjacencyOffsets> offsets) {
        var map = new ObjectObjectHashMap<String, AdjacencyOffsets>(offsets.size());
        offsets.forEach(map::put);
        return map;
    }

    CompositeRelationshipIterator concurrentCopy() {
        return new CompositeRelationshipIterator(adjacencyDegrees, adjacencyList, adjacencyOffsets, propertyLists, propertyOffsets);
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
        var degree = adjacencyDegrees.degree(sourceId);
        var adjacencyCursor = cursorCache.initializedTo(offset, degree);
        // init property cursors
        for (var propertyKey : propertyKeys) {
            var propertyOffset = propertyOffsets.get(propertyKey).get(sourceId);
            var propertyCursor = propertyCursorCache.get(propertyKey).init(propertyOffset, degree);
            propertyCursorCache.put(propertyKey, propertyCursor);
        }

        // in-step iteration of adjacency and property cursors
        while (adjacencyCursor.hasNextVLong()) {
            visitor.startId(sourceId);
            visitor.endId(adjacencyCursor.nextVLong());
            visitor.type(relType);

            propertyCursorCache.forEach((ObjectObjectProcedure<String, PropertyCursor>)
                (propertyKey, propertyCursor) -> visitor.property(
                    propertyKey,
                    Double.longBitsToDouble(propertyCursor.nextLong())
                ));

            visitor.endOfEntity();
        }
    }
}
