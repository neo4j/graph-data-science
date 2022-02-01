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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compress.AdjacencyListBehavior;
import org.neo4j.gds.core.huge.DirectIdMap;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public abstract class AdjacencyListBuilderBaseTest {

    protected void testAdjacencyList() {
        var nodeCount = 6;

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(RelationshipProjection.of("", Orientation.NATURAL, Aggregation.NONE))
            .aggregations(new Aggregation[]{Aggregation.NONE})
            .propertyKeyIds(new int[0])
            .defaultValues(new double[0])
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .preAggregate(false).build();

        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            () -> nodeCount,
            PropertyMappings.of(),
            importMetaData.aggregations(),
            AllocationTracker.empty()
        );

        AdjacencyBuffer adjacencyBuffer = new AdjacencyBufferBuilder()
            .adjacencyCompressorFactory(adjacencyCompressorFactory)
            .importMetaData(importMetaData)
            .importSizing(ImportSizing.of(1, nodeCount))
            .allocationTracker(AllocationTracker.empty())
            .build();

        DirectIdMap idMap = new DirectIdMap(nodeCount);

        RelationshipsBatchBuffer relationshipsBatchBuffer = new RelationshipsBatchBuffer(idMap, -1, 10);
        Map<Long, Long> relationships = new HashMap<>();
        for (long i = 0; i < nodeCount; i++) {
            relationships.put(i, nodeCount - i);
            relationshipsBatchBuffer.add(i, nodeCount - i);
        }

        var importer = ThreadLocalSingleTypeRelationshipImporter.of(
            adjacencyBuffer,
            relationshipsBatchBuffer,
            importMetaData,
            null,
            AllocationTracker.empty()
        );
        importer.importRelationships();

        adjacencyBuffer.adjacencyListBuilderTasks(Optional.empty()).forEach(Runnable::run);

        try (var adjacencyList = adjacencyCompressorFactory.build().adjacency()) {
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

        AdjacencyPreAggregation.preAggregate(values, properties, 0, values.length, aggregations);

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
