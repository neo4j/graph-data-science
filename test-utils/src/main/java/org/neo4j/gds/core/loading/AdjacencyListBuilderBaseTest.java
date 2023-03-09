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

import org.assertj.core.data.Offset;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.api.compress.AdjacencyCompressor.ValueMapper;
import org.neo4j.gds.compat.LongPropertyReference;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.DirectIdMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public abstract class AdjacencyListBuilderBaseTest {

    private void adjacencyListTest(Optional<Long> idOffset) {
        long nodeCount = 6;

        var fakeNodeCount = idOffset.map(o -> nodeCount + o).orElse(nodeCount);
        var mapper = idOffset.map(offset -> (ValueMapper) value -> value + offset);
        var toMapped = mapper.orElseGet(() -> id -> id);

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(RelationshipProjection.of("", Orientation.NATURAL, Aggregation.NONE))
            .aggregations(Aggregation.NONE)
            .propertyKeyIds()
            .defaultValues()
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .build();


        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            () -> fakeNodeCount,
            PropertyMappings.of(),
            importMetaData.aggregations()
        );

        AdjacencyBuffer adjacencyBuffer = new AdjacencyBufferBuilder()
            .adjacencyCompressorFactory(adjacencyCompressorFactory)
            .importMetaData(importMetaData)
            .importSizing(ImportSizing.of(1, nodeCount))
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
            null
        );
        importer.importRelationships();

        LongConsumer drainCountConsumer = drainCount -> assertThat(drainCount).isGreaterThan(0);
        adjacencyBuffer.adjacencyListBuilderTasks(mapper, Optional.of(drainCountConsumer)).forEach(Runnable::run);

        var adjacencyList = adjacencyCompressorFactory.build().adjacency();
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            var cursor = adjacencyList.adjacencyCursor(toMapped.map(nodeId));
            while (cursor.hasNextVLong()) {
                long target = cursor.nextVLong();
                var expected = toMapped.map(relationships.remove(nodeId));
                assertEquals(expected, target);
            }
        }
        assertThat(relationships).isEmpty();
    }

    private void adjacencyListWithPropertiesTest(Optional<Long> idOffset) {
        long nodeCount = 6;

        var rng = new Random(42);

        var fakeNodeCount = idOffset.map(o -> nodeCount + o).orElse(nodeCount);
        var mapper = idOffset.map(offset -> (ValueMapper) value -> value + offset);
        var toMapped = mapper.orElseGet(() -> id -> id);

        // Load a single property
        int propertyKeyId = 0;
        double defaultValue = 42.0;
        var propertyMappings = PropertyMappings.of(PropertyMapping.of("foo", defaultValue));
        var relationshipProjection = RelationshipProjection
            .of("", Orientation.NATURAL, Aggregation.NONE)
            .withAdditionalPropertyMappings(propertyMappings);

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(relationshipProjection)
            .aggregations(Aggregation.NONE)
            .propertyKeyIds(propertyKeyId)
            .defaultValues(defaultValue)
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .build();

        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            () -> fakeNodeCount,
            propertyMappings,
            importMetaData.aggregations()
        );

        AdjacencyBuffer adjacencyBuffer = new AdjacencyBufferBuilder()
            .adjacencyCompressorFactory(adjacencyCompressorFactory)
            .importMetaData(importMetaData)
            .importSizing(ImportSizing.of(1, nodeCount))
            .build();

        DirectIdMap idMap = new DirectIdMap(nodeCount);

        RelationshipsBatchBuffer relationshipsBatchBuffer = new RelationshipsBatchBuffer(idMap, -1, 10);
        PropertyReader.Buffered propertyReader = PropertyReader.buffered((int) nodeCount, 1);

        Map<Long, Long> expectedRelationships = new HashMap<>();
        Map<Integer, Double> expectedRelationshipProperties = new HashMap<>();

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            long targetId = nodeCount - nodeId;
            int relationshipId = (int) nodeId;
            double relationshipProperty = rng.nextDouble();

            expectedRelationships.put(nodeId, targetId);
            relationshipsBatchBuffer.add(
                nodeId,
                targetId,
                relationshipId,
                // PropertyReader.Buffered does not require property references
                LongPropertyReference.empty()
            );

            expectedRelationshipProperties.put(relationshipId, relationshipProperty);
            propertyReader.add(relationshipId, propertyKeyId, relationshipProperty);
        }

        var importer = ThreadLocalSingleTypeRelationshipImporter.of(
            adjacencyBuffer,
            relationshipsBatchBuffer,
            importMetaData,
            propertyReader
        );

        // import target ids into intermediate buffers (ChunkedAdjacencyList)
        importer.importRelationships();

        // import targets ids from intermediate buffer to final adjacency list
        LongConsumer drainCountConsumer = drainCount -> assertThat(drainCount).isGreaterThan(0);
        adjacencyBuffer.adjacencyListBuilderTasks(mapper, Optional.of(drainCountConsumer)).forEach(Runnable::run);

        var adjacencyListsWithProperties = adjacencyCompressorFactory.build();
        var adjacencyList = adjacencyListsWithProperties.adjacency();
        var propertyLists = adjacencyListsWithProperties.properties();
        assertThat(propertyLists).hasSize(1);
        var propertyList = propertyLists.get(0);

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            var adjacencyCursor = adjacencyList.adjacencyCursor(toMapped.map(nodeId));
            while (adjacencyCursor.hasNextVLong()) {
                long target = adjacencyCursor.nextVLong();
                var expected = toMapped.map(expectedRelationships.remove(nodeId));
                assertEquals(expected, target);
            }

            int relationshipId = (int) nodeId;
            var cursor = propertyList.propertyCursor(toMapped.map(nodeId));
            while (cursor.hasNextLong()) {
                double property = Double.longBitsToDouble(cursor.nextLong());
                double expected = expectedRelationshipProperties.remove(relationshipId);
                assertThat(property).isCloseTo(expected, Offset.offset(1E-3));
            }
        }

        assertThat(expectedRelationships).isEmpty();
        assertThat(expectedRelationshipProperties).isEmpty();
    }

    void testAdjacencyList() {
        adjacencyListTest(Optional.empty());
    }

    void testAdjacencyListWithProperties() {
        adjacencyListWithPropertiesTest(Optional.empty());
    }

    void testValueMapper() {
        adjacencyListTest(Optional.of(10000L));
    }

    void testValueMapperWithProperties() {
        adjacencyListWithPropertiesTest(Optional.of(10000L));
    }
}
