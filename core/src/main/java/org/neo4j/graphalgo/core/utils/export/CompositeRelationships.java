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
package org.neo4j.graphalgo.core.utils.export;

import org.neo4j.graphalgo.core.huge.TransientAdjacencyList;
import org.neo4j.graphalgo.core.huge.TransientAdjacencyOffsets;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class CompositeRelationships {

    private final TransientAdjacencyList adjacencyList;
    private final TransientAdjacencyOffsets adjacencyOffsets;

    private final Map<String, TransientAdjacencyList> propertyLists;
    private final Map<String, TransientAdjacencyOffsets> propertyOffsets;

    private final TransientAdjacencyList.DecompressingCursor cursorCache;
    private final Map<String, TransientAdjacencyList.Cursor> propertyCursorCache;

    CompositeRelationships(
        TransientAdjacencyList adjacencyList,
        TransientAdjacencyOffsets adjacencyOffsets,
        Map<String, TransientAdjacencyList> propertyLists,
        Map<String, TransientAdjacencyOffsets> propertyOffsets
    ) {
        this.adjacencyList = adjacencyList;
        this.adjacencyOffsets = adjacencyOffsets;
        this.propertyLists = propertyLists;
        this.propertyOffsets = propertyOffsets;
        this.cursorCache = adjacencyList.rawDecompressingCursor();
        this.propertyCursorCache = propertyLists.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().rawCursor()
        ));
    }

    int propertyCount() {
        return propertyLists.size();
    }

    void visitRelationships(long sourceId, String relType, InputEntityVisitor visitor) throws IOException {
        var offset = adjacencyOffsets.get(sourceId);

        if (offset == 0L) {
            return;
        }

        var adjacencyCursor = TransientAdjacencyList.decompressingCursor(cursorCache, offset);

        propertyLists.forEach((propertyKey, propertyList) -> propertyCursorCache
            .computeIfPresent(
                propertyKey,
                (ignore, cursor) -> TransientAdjacencyList.cursor(
                    cursor,
                    propertyOffsets.get(propertyKey).get(sourceId)
                )
            ));

        while (adjacencyCursor.hasNextVLong()) {
            long targetId = adjacencyCursor.nextVLong();
            visitor.startId(sourceId);
            visitor.endId(targetId);
            visitor.type(relType);

            propertyCursorCache.forEach((propertyKey, propertyCursor) -> {
                visitor.property(propertyKey, Double.longBitsToDouble(propertyCursor.nextLong()));
            });

            visitor.endOfEntity();
        }
    }

    CompositeRelationships concurrentCopy() {
        return new CompositeRelationships(adjacencyList, adjacencyOffsets, propertyLists, propertyOffsets);
    }
}
