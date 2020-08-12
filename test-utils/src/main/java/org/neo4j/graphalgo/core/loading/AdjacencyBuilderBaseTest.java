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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdjacencyBuilderBaseTest {

    protected void testAdjacencyList(
        AdjacencyListBuilderFactory listBuilderFactory,
        AdjacencyOffsetsFactory offsetsFactory
    ) {
        RelationshipsBuilder globalBuilder = new RelationshipsBuilder(
            RelationshipProjection.of("", Orientation.UNDIRECTED, Aggregation.NONE),
            listBuilderFactory,
            offsetsFactory
        );
        AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
            globalBuilder,
            1,
            8,
            AllocationTracker.EMPTY,
            new LongAdder(),
            new int[0],
            new double[0],
            new Aggregation[]{Aggregation.NONE}
        );
        long nodeCount = 6;
        DirectIdMapping idMapping = new DirectIdMapping(nodeCount);

        RelationshipsBatchBuffer relationshipsBatchBuffer = new RelationshipsBatchBuffer(idMapping, -1, 10);
        Map<Long, Long> relationships = new HashMap<>();
        for (long i = 0; i < nodeCount; i++) {
            relationships.put(i, nodeCount - i);
            relationshipsBatchBuffer.add(i, nodeCount - i, -1);
        }

        RelationshipImporter relationshipImporter = new RelationshipImporter(AllocationTracker.EMPTY, adjacencyBuilder);
        RelationshipImporter.Imports imports = relationshipImporter.imports(Orientation.NATURAL, false);
        imports.importRelationships(relationshipsBatchBuffer, null);

        adjacencyBuilder.flushTasks().forEach(Runnable::run);

        try (
            var adjacencyOffsets = globalBuilder.globalAdjacencyOffsets();
            var adjacencyList = globalBuilder.adjacencyList()
        ) {
            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
                long offset = adjacencyOffsets.get(nodeId);
                try (var cursor = adjacencyList.decompressingCursor(offset)) {
                    while (cursor.hasNextVLong()) {
                        long target = cursor.nextVLong();
                        assertEquals(relationships.remove(nodeId), target);
                    }
                }
            }
            assertTrue(relationships.isEmpty());
        }
    }
}
