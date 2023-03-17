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
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.compress.AdjacencyCompressor.ValueMapper;
import org.neo4j.gds.compat.LongPropertyReference;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.DirectIdMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

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

        long fakeNodeCount = idOffset.map(o -> nodeCount + o).orElse(nodeCount);
        var mapper = idOffset.map(offset -> (ValueMapper) value -> value + offset);
        var toOriginal = mapper.orElseGet(() -> id -> id);

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

        List<Long> expectedRelationships = new ArrayList<>();
        List<Double> expectedRelationshipProperties = new ArrayList<>();
        Map<Long, Integer> targetsToRelationshipId = new HashMap<>();

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            long targetId = nodeCount - nodeId;
            long originalTargetId = toOriginal.map(targetId);
            var relationshipId = expectedRelationships.size();
            assertThat(expectedRelationshipProperties.size()).isEqualTo(relationshipId);

            targetsToRelationshipId.put(originalTargetId, relationshipId);
            double relationshipProperty = rng.nextDouble();

            relationshipsBatchBuffer.add(
                nodeId,
                targetId,
                relationshipId,
                // PropertyReader.Buffered does not require property references
                LongPropertyReference.empty()
            );

            propertyReader.add(relationshipId, propertyKeyId, relationshipProperty);

            expectedRelationships.add(targetId);
            expectedRelationshipProperties.add(relationshipProperty);
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

        assertTopologyAndProperties(
            nodeCount,
            toOriginal,
            targetsToRelationshipId,
            expectedRelationships,
            expectedRelationshipProperties,
            adjacencyList,
            propertyList
        );
    }

    private void adjacencyListWithAggregationsTest() {
        long nodeCount = 6;

        var rng = new Random(42);

        // Load multiple properties with different aggregations
        Aggregation[] aggregations = new Aggregation[]{Aggregation.SINGLE, Aggregation.SUM, Aggregation.MAX};
        int propertyCount = aggregations.length;

        double defaultValue = 42.0;
        int degree = 5;
        int relationshipCount = (int) (nodeCount * degree);
        var propertyKeyIds = IntStream.range(0, propertyCount).toArray();
        var propertyMappings = PropertyMappings.of(Arrays
            .stream(aggregations)
            .map(agg -> PropertyMapping.of("foo_" + agg.name(), DefaultValue.of(defaultValue), agg))
            .toArray(PropertyMapping[]::new));
        var defaultValues = DoubleStream.generate(() -> defaultValue).limit(propertyCount).toArray();

        var relationshipProjection = RelationshipProjection
            .of("", Orientation.NATURAL, Aggregation.NONE)
            .withAdditionalPropertyMappings(propertyMappings);

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(relationshipProjection)
            .aggregations(aggregations)
            .propertyKeyIds(propertyKeyIds)
            .defaultValues(defaultValues)
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .build();

        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            () -> nodeCount,
            propertyMappings,
            importMetaData.aggregations()
        );

        AdjacencyBuffer adjacencyBuffer = new AdjacencyBufferBuilder()
            .adjacencyCompressorFactory(adjacencyCompressorFactory)
            .importMetaData(importMetaData)
            .importSizing(ImportSizing.of(1, nodeCount))
            .build();

        DirectIdMap idMap = new DirectIdMap(nodeCount);

        RelationshipsBatchBuffer relationshipsBatchBuffer = new RelationshipsBatchBuffer(idMap, -1, relationshipCount);
        PropertyReader.Buffered propertyReader = PropertyReader.buffered(relationshipCount, propertyCount);

        Map<Long, Integer> targetsToRelationshipId = new HashMap<>();
        List<Long> expectedRelationships = new ArrayList<>();
        Map<Aggregation, Map<Integer, Double>> expectedRelationshipProperties = new HashMap<>();
        Arrays.stream(aggregations).forEach(agg -> expectedRelationshipProperties.put(agg, new HashMap<>()));

        var importer = ThreadLocalSingleTypeRelationshipImporter.of(
            adjacencyBuffer,
            relationshipsBatchBuffer,
            importMetaData,
            propertyReader
        );

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            long targetId = nodeCount - nodeId;

            int aggregatedRelationshipId = expectedRelationships.size();
            expectedRelationships.add(targetId);
            targetsToRelationshipId.put(targetId, aggregatedRelationshipId);
        }

        // we import parallel relationships but we want to avoid triggering the pre-aggregation
        // for that reason, we are importing only one relationship per node in one buffer flush
        int localRelationshipId = 0;
        for (int i = 0; i < degree; i++) {
            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {

                // get the relationship id of the target node
                long targetId = nodeCount - nodeId;
                int aggregatedRelationshipId = targetsToRelationshipId.get(targetId);

                double relationshipProperty = rng.nextDouble();

                // add this relationship to the buffers
                relationshipsBatchBuffer.add(
                    nodeId,
                    targetId,
                    localRelationshipId,
                    // PropertyReader.Buffered does not require property references
                    LongPropertyReference.empty()
                );
                for (int propertyKeyId : propertyKeyIds) {
                    propertyReader.add(localRelationshipId, propertyKeyId, relationshipProperty);
                }

                // update expected properties where we manually aggregate the expected value
                for (Aggregation aggregation : aggregations) {
                    expectedRelationshipProperties
                        .get(aggregation)
                        .compute(aggregatedRelationshipId, (relId, current) -> {
                            var normalizedValue = aggregation.normalizePropertyValue(relationshipProperty);
                            return current == null
                                ? aggregation.emptyValue(normalizedValue)
                                : aggregation.merge(current, normalizedValue);
                        });
                }

                localRelationshipId++;
            }

            // import target ids and properties into intermediate buffers (ChunkedAdjacencyList)
            importer.importRelationships();
            importer.buffer().reset();
        }

        // import targets ids from intermediate buffer to final adjacency list
        LongConsumer drainCountConsumer = drainCount -> assertThat(drainCount).isGreaterThan(0);
        adjacencyBuffer
            .adjacencyListBuilderTasks(Optional.empty(), Optional.of(drainCountConsumer))
            .forEach(Runnable::run);

        var adjacencyListsWithProperties = adjacencyCompressorFactory.build();
        var adjacencyList = adjacencyListsWithProperties.adjacency();
        var propertyLists = adjacencyListsWithProperties.properties();
        assertThat(propertyLists).hasSize(aggregations.length);

        for (int i = 0; i < aggregations.length; i++) {
            var actualProperties = propertyLists.get(i);

            var aggregatedProperties = expectedRelationshipProperties.get(aggregations[i]);
            var expectedProperties = IntStream.range(0, expectedRelationships.size())
                .mapToObj(aggregatedProperties::get)
                .collect(Collectors.toList());

            assertTopologyAndProperties(
                nodeCount,
                id -> id,
                targetsToRelationshipId,
                expectedRelationships,
                expectedProperties,
                adjacencyList,
                actualProperties
            );
        }
    }

    private static void assertTopologyAndProperties(
        long nodeCount,
        ValueMapper toMapped,
        Map<Long, Integer> targetNodesToRelationshipId,
        List<Long> expectedRelationships,
        List<Double> expectedRelationshipProperties,
        AdjacencyList adjacencyList,
        AdjacencyProperties propertyList
    ) {
        // we modify the incoming data structures, so we need to copy them
        targetNodesToRelationshipId = new HashMap<>(targetNodesToRelationshipId);
        expectedRelationships = new ArrayList<>(expectedRelationships);
        expectedRelationshipProperties = new ArrayList<>(expectedRelationshipProperties);

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            var mappedNodeId = toMapped.map(nodeId);

            var adjacencyCursor = adjacencyList.adjacencyCursor(mappedNodeId);
            while (adjacencyCursor.hasNextVLong()) {
                long target = adjacencyCursor.nextVLong();

                var relationshipId = targetNodesToRelationshipId.remove(target);
                assertThat(relationshipId).isNotNull();

                var expected = toMapped.map(expectedRelationships.set(relationshipId, -1L));
                assertEquals(expected, target);

                var propertyCursor = propertyList.propertyCursor(mappedNodeId);
                while (propertyCursor.hasNextLong()) {
                    double property = Double.longBitsToDouble(propertyCursor.nextLong());
                    double expectedValue = expectedRelationshipProperties.set(relationshipId, Double.NaN);
                    assertThat(property).isCloseTo(expectedValue, Offset.offset(1E-3));
                }
            }
        }

        assertThat(expectedRelationships).isNotEmpty().allMatch(v -> v == -1L);
        assertThat(expectedRelationshipProperties).isNotEmpty().allMatch(v -> Double.isNaN(v));
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

    void testAdjacencyListWithAggregations() {
        adjacencyListWithAggregationsTest();
    }
}
