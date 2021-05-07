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
package org.neo4j.graphalgo.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.DirectIdMapping;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.core.loading.AdjacencyBuilder.IGNORE_VALUE;

public abstract class AdjacencyBuilderBaseTest {

    protected void testAdjacencyList(AdjacencyBuilderFactory listBuilderFactory) {
        AdjacencyListWithPropertiesBuilder globalBuilder = AdjacencyListWithPropertiesBuilder.create(
            6,
            RelationshipProjection.of("", Orientation.UNDIRECTED, Aggregation.NONE),
            listBuilderFactory,
            new Aggregation[]{Aggregation.NONE},
            new int[0],
            new double[0],
            AllocationTracker.empty()
        );

        AdjacencyBuilder adjacencyBuilder = AdjacencyBuilder.compressing(
            globalBuilder,
            1,
            8,
            AllocationTracker.empty(),
            new LongAdder(),
            false
        );
        long nodeCount = 6;
        DirectIdMapping idMapping = new DirectIdMapping(nodeCount);

        RelationshipsBatchBuffer relationshipsBatchBuffer = new RelationshipsBatchBuffer(idMapping, -1, 10);
        Map<Long, Long> relationships = new HashMap<>();
        for (long i = 0; i < nodeCount; i++) {
            relationships.put(i, nodeCount - i);
            relationshipsBatchBuffer.add(i, nodeCount - i, -1);
        }

        RelationshipImporter relationshipImporter = new RelationshipImporter(
            AllocationTracker.empty(),
            adjacencyBuilder
        );
        RelationshipImporter.Imports imports = relationshipImporter.imports(Orientation.NATURAL, false);
        imports.importRelationships(relationshipsBatchBuffer, null);

        adjacencyBuilder.flushTasks().forEach(Runnable::run);

        try (var adjacencyList = globalBuilder.build().adjacency()) {
            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
                try (var cursor = adjacencyList.adjacencyCursor(nodeId)) {
                    while (cursor.hasNextVLong()) {
                        long target = cursor.nextVLong();
                        assertEquals(relationships.remove(nodeId), target);
                    }
                }
            }
            assertTrue(relationships.isEmpty());
        }
    }

    @Test
    // TODO test single case
    // TODO add testcase that sets a range
    void testAggregation() {
        var values = new long[]{3, 1, 2, 2, 3, 1};

        var properties = new long[2][values.length];
        properties[0] = new long[]{1, 1, 1, 1, 1, 1};
        properties[1] = new long[]{1, 2, 3, 4, 5, 6};


        var aggregations = new Aggregation[]{Aggregation.SUM, Aggregation.MAX};

        AdjacencyBuilder.aggregate(values, properties, 0, values.length, aggregations);

        assertArrayEquals(
            new long[]{3, 1, 2, IGNORE_VALUE, IGNORE_VALUE, IGNORE_VALUE},
            values
        );

        assertArrayEquals(
            new long[]{2, 2, 2, 1, 1, 1},
            properties[0]
        );

        assertArrayEquals(
            new long[]{5, 6, 4, 4, 5, 6},
            properties[1]
        );
    }
}
