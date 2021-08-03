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
package org.neo4j.gds.core.loading.construction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.PropertySchema;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.loading.construction.NodesBuilder;
import org.neo4j.graphalgo.core.loading.construction.RelationshipsBuilder;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.fromGdl;

class GraphFactoryTest {

    private static final String EXPECTED_WITH_AGGREGATION =
        "(a)-[{w: 0.0}]->(b)-[{w: 2.0}]->(c)-[{w: 4.0}]->(d)-[{w: 6.0}]->(a)";

    private static final String EXPECTED_WITHOUT_AGGREGATION_GRAPH =
        "(a)-[{w: 0.0}]->(b)" +
        "(a)-[{w: 0.0}]->(b)" +
        "(b)-[{w: 1.0}]->(c)" +
        "(b)-[{w: 1.0}]->(c)" +
        "(c)-[{w: 2.0}]->(d)" +
        "(c)-[{w: 2.0}]->(d)" +
        "(d)-[{w: 3.0}]->(a)" +
        "(d)-[{w: 3.0}]->(a)";

    private static Graph expectedWithAggregation(Orientation orientation) {
        return fromGdl(EXPECTED_WITH_AGGREGATION, orientation);
    }

    private static Graph expectedWithoutAggregation(Orientation orientation) {
        return fromGdl(EXPECTED_WITHOUT_AGGREGATION_GRAPH, orientation);
    }

    static Stream<Orientation> validProjections() {
        return Stream.of(Orientation.NATURAL, Orientation.REVERSE);
    }

    static Stream<Arguments> orientationsAndIdMaps() {
        return crossArguments(
            () -> validProjections().map(Arguments::of),
            () -> TestMethodRunner.idMapImplementation().map(Arguments::of)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orientationsAndIdMaps")
    void unweighted(Orientation orientation, TestMethodRunner runTest) {
        runTest.run(() -> {
            long nodeCount = 4;
            var nodesBuilder = GraphFactory.initNodesBuilder()
                .maxOriginalId(nodeCount)
                .hasLabelInformation(true)
                .tracker(AllocationTracker.empty())
                .build();

            nodesBuilder.addNode(0, NodeLabel.of("A"));
            nodesBuilder.addNode(1, NodeLabel.of("A"), NodeLabel.of("B"));
            nodesBuilder.addNode(2, NodeLabel.of("C"));
            nodesBuilder.addNode(3);

            NodeMapping idMap = nodesBuilder.build().nodeMapping();
            RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
                .nodes(idMap)
                .orientation(orientation)
                .aggregation(Aggregation.SUM)
                .tracker(AllocationTracker.empty())
                .build();

            for (int i = 0; i < nodeCount; i++) {
                relationshipsBuilder.add(i, (i + 1) % nodeCount);
            }
            Graph graph = GraphFactory.create(
                idMap,
                relationshipsBuilder.build(),
                AllocationTracker.empty()
            );

            var expectedGraph = fromGdl("(a:A)-->(b:A:B)-->(c:C)-->(d)-->(a)", orientation);

            assertGraphEquals(expectedGraph, graph);
            assertEquals(nodeCount, graph.relationshipCount());
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orientationsAndIdMaps")
    void withNodeProperties(Orientation orientation, TestMethodRunner runTest) {
        var expectedGraph = fromGdl("(a:A {p: 42L})-->(b:A:B {p: 1337L})-->(c:C {p: 13L})-->(d {p: 33L})-->(a)", orientation);
        runTest.run(() -> {
            long nodeCount = expectedGraph.nodeCount();
            var nodesBuilder = GraphFactory.initNodesBuilder(expectedGraph.schema().nodeSchema())
                .maxOriginalId(nodeCount)
                .nodeCount(nodeCount)
                .tracker(AllocationTracker.empty())
                .build();

            nodesBuilder.addNode(0, Map.of("p", Values.longValue(42)), NodeLabel.of("A"));
            nodesBuilder.addNode(1, Map.of("p", Values.longValue(1337)), NodeLabel.of("A"), NodeLabel.of("B"));
            nodesBuilder.addNode(2, Map.of("p", Values.longValue(13)), NodeLabel.of("C"));
            nodesBuilder.addNode(3, Map.of("p", Values.longValue(33)));

            Graph graph = buildGraphFromNodesBuilder(
                orientation,
                expectedGraph.schema().nodeSchema(),
                nodeCount,
                nodesBuilder
            );

            assertGraphEquals(expectedGraph, graph);
            assertEquals(nodeCount, graph.relationshipCount());
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orientationsAndIdMaps")
    void withNodePropertiesWithDefaultValue(Orientation orientation, TestMethodRunner runTest) {
        var expectedGraph = fromGdl("(a:A {p: 42L})-->(b:A {p: -1L})-->(c:B {p: -42L})-->(a)", orientation);
        var nodeSchema = NodeSchema.builder()
            .addProperty(
                NodeLabel.of("A"),
                "p",
                PropertySchema.of("p", ValueType.LONG, DefaultValue.of(-1L), GraphStore.PropertyState.TRANSIENT)
            )
            .addProperty(
                NodeLabel.of("B"),
                "p",
                PropertySchema.of("p", ValueType.LONG, DefaultValue.of(-42L), GraphStore.PropertyState.TRANSIENT)
            )
            .build();
        runTest.run(() -> {
            int nodeCount = (int) expectedGraph.nodeCount();
            var nodesBuilder = GraphFactory.initNodesBuilder(nodeSchema)
                .maxOriginalId(nodeCount)
                .nodeCount(nodeCount)
                .tracker(AllocationTracker.empty())
                .build();

            nodesBuilder.addNode(0, Map.of("p", Values.longValue(42)), NodeLabel.of("A"));
            nodesBuilder.addNode(1, NodeLabel.of("A"));
            nodesBuilder.addNode(2, NodeLabel.of("B"));

            Graph graph = buildGraphFromNodesBuilder(orientation, nodeSchema, nodeCount, nodesBuilder);

            assertGraphEquals(expectedGraph, graph);
            assertEquals(nodeCount, graph.relationshipCount());
        });
    }

    private static Graph buildGraphFromNodesBuilder(
        Orientation orientation,
        NodeSchema nodeSchema,
        long nodeCount,
        NodesBuilder nodesBuilder
    ) {
        var nodeMappingAndProperties = nodesBuilder.build();
        var idMap = nodeMappingAndProperties.nodeMapping();
        var nodeProperties = NodePropertiesTestHelper.unionNodePropertiesOrThrow(nodeMappingAndProperties);
        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(orientation)
            .aggregation(Aggregation.SUM)
            .tracker(AllocationTracker.empty())
            .build();

        for (int i = 0; i < nodeCount; i++) {
            relationshipsBuilder.add(i, (i + 1) % nodeCount);
        }
        return GraphFactory.create(
            idMap,
            nodeSchema,
            nodeProperties,
            RelationshipType.ALL_RELATIONSHIPS,
            relationshipsBuilder.build(),
            AllocationTracker.empty()
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orientationsAndIdMaps")
    void weightedWithAggregation(Orientation orientation, TestMethodRunner runTest) {
        runTest.run(() -> {
            var expected = expectedWithAggregation(orientation);

            Graph graph = generateGraph(orientation, Aggregation.SUM);
            assertGraphEquals(expected, graph);
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orientationsAndIdMaps")
    void weightedWithoutAggregation(Orientation orientation, TestMethodRunner runTest) {
        runTest.run(() -> {
            Graph graph = generateGraph(orientation, Aggregation.NONE);
            assertGraphEquals(expectedWithoutAggregation(orientation), graph);
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void undirectedWithAggregation(TestMethodRunner runTest) {
        runTest.run(() -> {
            Graph graph = generateGraph(Orientation.UNDIRECTED, Aggregation.SUM);
            assertGraphEquals(expectedWithAggregation(Orientation.UNDIRECTED), graph);
        });
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.core.loading.construction.TestMethodRunner#idMapImplementation")
    void undirectedWithoutAggregation(TestMethodRunner runTest) {
        runTest.run(() -> {
            Graph graph = generateGraph(Orientation.UNDIRECTED, Aggregation.NONE);
            assertGraphEquals(expectedWithoutAggregation(Orientation.UNDIRECTED), graph);
        });
    }

    private Graph generateGraph(Orientation orientation, Aggregation aggregation) {
        long nodeCount = 4;
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(nodeCount)
            .tracker(AllocationTracker.empty())
            .build();

        for (int i = 0; i < nodeCount; i++) {
            nodesBuilder.addNode(i);
        }

        NodeMapping idMap = nodesBuilder.build().nodeMapping();
        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(orientation)
            .addPropertyConfig(aggregation, DefaultValue.forDouble())
            .tracker(AllocationTracker.empty())
            .build();

        for (int i = 0; i < nodeCount; i++) {
            relationshipsBuilder.add(i, (i + 1) % nodeCount, i);
            relationshipsBuilder.add(i, (i + 1) % nodeCount, i);
        }
        return GraphFactory.create(idMap, relationshipsBuilder.build(), AllocationTracker.empty());
    }
}
