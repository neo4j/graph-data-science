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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.compress.AdjacencyCompressor.ValueMapper;
import org.neo4j.gds.api.compress.AdjacencyListsWithProperties;
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
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

public abstract class AdjacencyListBuilderBaseTest {

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

    private void adjacencyListTest(Optional<Long> idOffset) {
        var graph = generateGraph(6, 1, idOffset);
        assertTopologyAndProperties(graph);
    }

    private void adjacencyListWithPropertiesTest(Optional<Long> idOffset) {
        var graph = generateGraph(6, 1, idOffset, Aggregation.SINGLE);
        assertTopologyAndProperties(graph);
    }

    private void adjacencyListWithAggregationsTest() {
        var aggregations = new Aggregation[]{Aggregation.SINGLE, Aggregation.SUM, Aggregation.MAX};
        var graph = generateGraph(6, 5, Optional.empty(), aggregations);
        assertTopologyAndProperties(graph);
    }

    private static void assertTopologyAndProperties(GraphStructures graph) {
        var graphAssertions = graph.assertions();
        for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            var originalNodeId = graph.toOriginal(nodeId);

            var adjacencyCursor = graph.adjacencyList().adjacencyCursor(originalNodeId);
            while (adjacencyCursor.hasNextVLong()) {
                long target = adjacencyCursor.nextVLong();

                long expected = graph.toOriginal(graphAssertions.expectedTargetFor(nodeId));
                assertEquals(expected, target);
            }
        }

        for (var propertyStructure : graph.properties()) {
            AdjacencyProperties propertyList = propertyStructure.adjacencyProperties();
            var propertyAssertions = propertyStructure.assertions();

            for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                var originalNodeId = graph.toOriginal(nodeId);
                int relationshipId = graphAssertions.relationshipIdFor(nodeId);
                var propertyCursor = propertyList.propertyCursor(originalNodeId);

                while (propertyCursor.hasNextLong()) {
                    double property = Double.longBitsToDouble(propertyCursor.nextLong());
                    double expectedValue = propertyAssertions.expectedPropertyFor(relationshipId);
                    assertThat(property).isCloseTo(expectedValue, Offset.offset(1E-3));
                }
            }
            propertyAssertions.assertAllConsumed();
        }

        graphAssertions.assertAllConsumed();
    }

    // Generate a graph where every node is connected to only one other node.
    // There can be multiple relationships between the same pair of nodes.
    private GraphStructures generateGraph(
        long nodeCount,
        int parallelDegree,
        Optional<Long> idOffset,
        Aggregation... propertyAggregations
    ) {
        assert parallelDegree >= 1;

        var originalNodeCount = idOffset.map(o -> nodeCount + o).orElse(nodeCount);
        var mapper = idOffset.map(offset -> (ValueMapper) value -> value + offset);
        var toOriginal = mapper.orElseGet(() -> id -> id);

        int relationshipCount = (int) (nodeCount * parallelDegree);

        // Create properties for every provided aggregation
        int propertyCount = propertyAggregations.length;

        double defaultValue = 42.0;
        var propertyKeyIds = IntStream.range(0, propertyCount).toArray();
        var propertyMappings = PropertyMappings.of(Arrays
            .stream(propertyAggregations)
            .map(agg -> PropertyMapping.of("foo_" + agg.name(), DefaultValue.of(defaultValue), agg))
            .toArray(PropertyMapping[]::new));
        var defaultValues = DoubleStream.generate(() -> defaultValue).limit(propertyCount).toArray();

        var relationshipProjection = RelationshipProjection
            .of("", Orientation.NATURAL, Aggregation.NONE)
            .withAdditionalPropertyMappings(propertyMappings);

        var importAggregations = propertyAggregations.length == 0
            ? new Aggregation[]{Aggregation.NONE}
            : propertyAggregations;

        var importMetaData = ImmutableImportMetaData.builder()
            .projection(relationshipProjection)
            .aggregations(importAggregations)
            .propertyKeyIds(propertyKeyIds)
            .defaultValues(defaultValues)
            .typeTokenId(NO_SUCH_RELATIONSHIP_TYPE)
            .build();

        var adjacencyCompressorFactory = AdjacencyListBehavior.asConfigured(
            () -> originalNodeCount,
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

        Map<Long, Long> sourceNodeToTargetNode = new HashMap<>();
        Map<Long, Integer> sourceNodeToRelationshipId = new HashMap<>();
        Map<Aggregation, Map<Integer, Double>> expectedRelationshipProperties = new HashMap<>();
        Arrays.stream(propertyAggregations).forEach(agg -> expectedRelationshipProperties.put(agg, new HashMap<>()));

        var importer = ThreadLocalSingleTypeRelationshipImporter.of(
            adjacencyBuffer,
            relationshipsBatchBuffer,
            importMetaData,
            propertyReader
        );

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            long targetId = nodeCount - nodeId;

            int aggregatedRelationshipId = sourceNodeToTargetNode.size();
            sourceNodeToTargetNode.put(nodeId, targetId);
            sourceNodeToRelationshipId.put(nodeId, aggregatedRelationshipId);
        }

        var rng = new Random(42);

        // we import parallel relationships but we want to avoid triggering the pre-aggregation
        // for that reason, we are importing only one relationship per node in one buffer flush
        int localRelationshipId = 0;
        for (int i = 0; i < parallelDegree; i++) {
            for (long nodeId = 0; nodeId < nodeCount; nodeId++) {

                // get the relationship id of the target node
                long targetId = nodeCount - nodeId;
                int aggregatedRelationshipId = sourceNodeToRelationshipId.get(nodeId);

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
                for (Aggregation aggregation : propertyAggregations) {
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
        adjacencyBuffer
            .adjacencyListBuilderTasks(mapper, Optional.empty())
            .forEach(Runnable::run);

        AdjacencyListsWithProperties adjacencyListsWithProperties = adjacencyCompressorFactory.build();
        AdjacencyList adjacencyList = adjacencyListsWithProperties.adjacency();
        List<AdjacencyProperties> propertyLists = adjacencyListsWithProperties.properties();
        assertThat(propertyLists).hasSize(propertyAggregations.length);

        var builder = ImmutableGraphStructures.builder()
            .nodeCount(nodeCount)
            .toOriginalMapper(toOriginal)
            .adjacencyList(adjacencyList)
            .sourceNodeToTargetNode(sourceNodeToTargetNode)
            .sourceNodeToRelationshipId(sourceNodeToRelationshipId);

        for (int i = 0; i < propertyAggregations.length; i++) {
            AdjacencyProperties actualProperties = propertyLists.get(i);

            Map<Integer, Double> aggregatedProperties = expectedRelationshipProperties.get(propertyAggregations[i]);
            List<Double> expectedProperties = IntStream.range(0, sourceNodeToTargetNode.size())
                .mapToObj(aggregatedProperties::get)
                .collect(Collectors.toList());

            var properties = ImmutableGraphPropertyStructures.builder()
                .adjacencyProperties(actualProperties)
                .expectedProperties(expectedProperties)
                .build();

            builder.addProperty(properties);
        }

        return builder.build();
    }

    @ValueClass
    interface GraphStructures {

        AdjacencyList adjacencyList();

        long nodeCount();

        ValueMapper toOriginalMapper();

        Map<Long, Long> sourceNodeToTargetNode();

        Map<Long, Integer> sourceNodeToRelationshipId();

        List<GraphPropertyStructures> properties();

        default long toOriginal(long nodeId) {
            return toOriginalMapper().map(nodeId);
        }

        default GraphStructureAssertions assertions() {
            return new GraphStructureAssertions(
                sourceNodeToTargetNode(),
                sourceNodeToRelationshipId()
            );
        }
    }

    @ValueClass
    interface GraphPropertyStructures {

        AdjacencyProperties adjacencyProperties();

        List<Double> expectedProperties();

        default GraphPropertyStructureAssertions assertions() {
            return new GraphPropertyStructureAssertions(
                expectedProperties()
            );
        }
    }

    static final class GraphStructureAssertions {

        private final Map<Long, Long> sourceNodeToTargetNode;
        private final Map<Long, Integer> sourceNodeToRelationshipId;

        GraphStructureAssertions(
            Map<Long, Long> sourceNodeToTargetNode,
            Map<Long, Integer> sourceNodeToRelationshipId
        ) {
            this.sourceNodeToTargetNode = new HashMap<>(sourceNodeToTargetNode);
            this.sourceNodeToRelationshipId = new HashMap<>(sourceNodeToRelationshipId);
        }

        long expectedTargetFor(long sourceNode) {
            var expectedTarget = this.sourceNodeToTargetNode.remove(sourceNode);
            assertThat(expectedTarget).isNotNull();
            return expectedTarget;
        }

        int relationshipIdFor(long source) {
            var relationshipId = this.sourceNodeToRelationshipId.get(source);
            assertThat(relationshipId).isNotNull();
            return relationshipId;
        }

        void assertAllConsumed() {
            assertThat(this.sourceNodeToTargetNode).isEmpty();
        }
    }

    static final class GraphPropertyStructureAssertions {

        private final List<Double> expectedProperties;

        GraphPropertyStructureAssertions(List<Double> expectedProperties) {
            this.expectedProperties = new ArrayList<>(expectedProperties);
        }

        double expectedPropertyFor(int relationshipId) {
            var expectedProperty = this.expectedProperties.set(relationshipId, Double.NaN);
            assertThat(expectedProperty).isNotNaN();
            return expectedProperty;
        }

        void assertAllConsumed() {
            assertThat(expectedProperties).isNotEmpty().allMatch(v -> Double.isNaN(v));
        }
    }
}
