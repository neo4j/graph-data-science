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
package org.neo4j.gds.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GraphFactoryTestSupport;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestGraphLoaderFactory;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.GraphFactoryTestSupport.AllGraphStoreFactoryTypesTest;
import static org.neo4j.gds.GraphFactoryTestSupport.FactoryType.NATIVE;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.TestSupport.toArguments;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.runInTransaction;
import static org.neo4j.gds.core.Aggregation.DEFAULT;
import static org.neo4j.gds.core.Aggregation.MAX;
import static org.neo4j.gds.core.Aggregation.MIN;
import static org.neo4j.gds.core.Aggregation.NONE;
import static org.neo4j.gds.core.Aggregation.SINGLE;
import static org.neo4j.gds.core.Aggregation.SUM;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphLoaderMultipleRelTypesAndPropertiesTest extends BaseTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (n1:Node1 {prop1: 1})" +
        ", (n2:Node2 {prop2: 2})" +
        ", (n3:Node3 {prop3: 3})" +
        ", (n1)-[:REL1 {prop1: 1}]->(n2)" +
        ", (n1)-[:REL2 {prop2: 2}]->(n3)" +
        ", (n2)-[:REL1 {prop3: 3, weight: 42}]->(n3)" +
        ", (n2)-[:REL3 {prop4: 4, weight: 1337}]->(n3)";

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @Test
    void nodeProjectionsWithExclusiveProperties() {
        GraphStore graphStore = new StoreLoaderBuilder()
            .putNodeProjectionsWithIdentifier(
                "N1",
                NodeProjection.of(
                    "Node1",
                    PropertyMappings.builder().addMapping(PropertyMapping.of("prop1", 0.0D)).build()
                )
            ).putNodeProjectionsWithIdentifier(
                "N2",
                NodeProjection.of(
                    "Node1",
                    PropertyMappings.of()
                )
            ).putNodeProjectionsWithIdentifier(
                "N3",
                NodeProjection.of(
                    "Node2",
                    PropertyMappings.builder().addMapping(PropertyMapping.of("prop2", 1.0D)).build()
                )
            ).graphName("myGraph")
            .databaseService(db)
            .build()
            .graphStore();

        assertEquals(Collections.singleton("prop1"), graphStore.nodePropertyKeys(NodeLabel.of("N1")));
        assertEquals(Collections.emptySet(), graphStore.nodePropertyKeys(NodeLabel.of("N2")));
        assertEquals(Collections.singleton("prop2"), graphStore.nodePropertyKeys(NodeLabel.of("N3")));

        NodePropertyValues prop1 = graphStore.nodeProperty("prop1").values();
        assertEquals(1L, prop1.longValue(0));
        assertEquals(0L, prop1.longValue(1));

        NodePropertyValues prop2 = graphStore.nodeProperty("prop2").values();
        assertEquals(1L, prop2.longValue(0));
        assertEquals(2L, prop2.longValue(1));
    }

    @Test
    void nodeProjectionsWithAndWithoutLabel() {
        NodeLabel allIdentifier = NodeLabel.of("ALL");
        NodeLabel node2Identifier = NodeLabel.of("Node2");


        GraphStore graphStore = new StoreLoaderBuilder()
            .putNodeProjectionsWithIdentifier(
                allIdentifier.name(),
                NodeProjection.of(
                    "*",
                    PropertyMappings.builder()
                        .addMapping(PropertyMapping.of("prop1", 42.0D))
                        .addMapping(PropertyMapping.of("prop2", 8.0D))
                        .build()
                )
            ).putNodeProjectionsWithIdentifier(
                node2Identifier.name(),
                NodeProjection.of(
                    "Node2",
                    PropertyMappings.builder().addMapping(PropertyMapping.of("prop2", 8.0D)).build()
                )
            ).graphName("myGraph")
            .databaseService(db)
            .build()
            .graphStore();

        assertEquals(Set.of("prop1", "prop2"), graphStore.nodePropertyKeys(allIdentifier));
        assertEquals(Set.of("prop2"), graphStore.nodePropertyKeys(node2Identifier));

        var allProp1 = graphStore.nodeProperty("prop1").values();
        var allProp2 = graphStore.nodeProperty("prop2").values();
        var node2Prop2 = graphStore.nodeProperty("prop2").values();

        LongStream.range(0, 3).forEach(nodeId -> {
            double allProp1Value = allProp1.doubleValue(nodeId);
            double allProp2Value = allProp2.doubleValue(nodeId);
            double node2Prop2Value = node2Prop2.doubleValue(nodeId);

            if (nodeId == 0) {
                assertEquals(1.0, allProp1Value);
                assertEquals(8.0, allProp2Value);
            } else if (nodeId == 1) {
                assertEquals(42.0, allProp1Value);
                assertEquals(2.0, allProp2Value);
                assertEquals(2.0, node2Prop2Value);
            } else {
                assertEquals(42.0, allProp1Value);
                assertEquals(8.0, allProp2Value);
            }
        });
    }

    @AllGraphStoreFactoryTypesTest
    void parallelRelationshipsWithoutProperties(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withDefaultAggregation(Aggregation.NONE)
            .graph();

        Graph expected = fromGdl(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-->(n2)" +
            "(n1)-->(n3)" +
            "(n2)-->(n3)" +
            "(n2)-->(n3)"
        );

        assertGraphEquals(expected, graph);
    }

    @AllGraphStoreFactoryTypesTest
    void parallelRelationships(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .withDefaultAggregation(NONE)
            .graph();

        Graph expected = fromGdl(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: 42.0d}]->(n3)" +
            "(n2)-[{weight: 1337.0d}]->(n3)"
        );

        assertGraphEquals(expected, graph);
    }

    static Stream<Arguments> deduplicateWithWeightsParams() {
        return crossArguments(toArguments(GraphFactoryTestSupport::allFactoryTypes), () -> Stream.of(
            Arguments.of(SUM, 1379.0),
            Arguments.of(MAX, 1337.0),
            Arguments.of(MIN, 42.0)
        ));
    }

    @ParameterizedTest
    @MethodSource("deduplicateWithWeightsParams")
    void parallelRelationshipsWithAggregation(
        GraphFactoryTestSupport.FactoryType factoryType,
        Aggregation aggregation,
        double expectedWeight
    ) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withDefaultAggregation(aggregation)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .graph();

        Graph expected = fromGdl(formatWithLocale(
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: %fd}]->(n3)", expectedWeight
        ));

        assertGraphEquals(expected, graph);
    }

    @Test
    void parallelRelationshipsWithAggregation_SINGLE() {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, NATIVE)
            .withDefaultAggregation(SINGLE)
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .graph();

        String expectedGraph =
            "(n1)" +
            "(n2)" +
            "(n3)" +
            "(n1)-[{weight: 1.0d}]->(n2)" +
            "(n1)-[{weight: 1.0d}]->(n3)" +
            "(n2)-[{weight: %fd}]->(n3)";

        Graph expected1 = fromGdl(formatWithLocale(expectedGraph, 42.0));
        Graph expected2 = fromGdl(formatWithLocale(expectedGraph, 1337.0));
        assertGraphEquals(Arrays.asList(expected1, expected2), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void multipleTypes(GraphFactoryTestSupport.FactoryType factoryType) {
        GraphStore graphStore = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("REL1", "REL2")
            .graphStore();

        assertEquals(2, graphStore.relationshipTypes().size());
        assertEquals(graphStore.relationshipTypes(), new HashSet<>(asList(
            RelationshipType.of("REL1"),
            RelationshipType.of("REL2")
        )));

        Graph rel1Graph = graphStore.getGraph(RelationshipType.of("REL1"));
        Graph rel2Graph = graphStore.getGraph(RelationshipType.of("REL2"));
        Graph unionGraph = graphStore.getGraph(RelationshipType.of("REL1"), RelationshipType.of("REL2"));

        assertGraphEquals(fromGdl("(a)-[:REL1]->(b)-[:REL1]->(c)"), rel1Graph);
        assertGraphEquals(fromGdl("(a)-[:REL2]->(c), (b)"), rel2Graph);
        assertGraphEquals(fromGdl("(a)-[:REL1]->(b)-[:REL1]->(c)<-[:REL2]-(a)"), unionGraph);
    }

    @AllGraphStoreFactoryTypesTest
    void multipleTypesWithProperties(GraphFactoryTestSupport.FactoryType factoryType) {
        GraphStore graphStore = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("REL1", "REL2")
            .withRelationshipProperties(PropertyMapping.of("prop1", 1337D))
            .graphStore();

        assertEquals(2, graphStore.relationshipTypes().size());
        assertEquals(graphStore.relationshipTypes(), new HashSet<>(asList(
            RelationshipType.of("REL1"),
            RelationshipType.of("REL2")
        )));

        Graph rel1Graph = graphStore.getGraph(RelationshipType.of("REL1"));
        Graph rel2Graph = graphStore.getGraph(RelationshipType.of("REL2"));
        Graph unionGraph = graphStore.getGraph(RelationshipType.of("REL1"), RelationshipType.of("REL2"));

        assertGraphEquals(fromGdl("(a)-[:REL1]->(b)-[:REL1]->(c)"), rel1Graph);
        assertGraphEquals(fromGdl("(a)-[:REL2]->(c), (b)"), rel2Graph);
        assertGraphEquals(fromGdl("(a)-[:REL1]->(b)-[:REL1]->(c)<-[:REL2]-(a)"), unionGraph);
    }

    @Test
    void multipleTypesWithDifferentProperties() {
        RelationshipType rel1 = RelationshipType.of("REL1");
        RelationshipType rel2 = RelationshipType.of("REL2");

        String prop1 = "prop1";
        String prop2 = "prop2";

        var graphStore = new StoreLoaderBuilder()
            .databaseService(db)
            .addRelationshipProjections(
                RelationshipProjection
                    .builder()
                    .type(rel1.name)
                    .addProperty(prop1, prop1, DefaultValue.of(Double.NaN))
                    .build(),
                RelationshipProjection
                    .builder()
                    .type(rel2.name)
                    .addProperty(prop2, prop2, DefaultValue.of(Double.NaN))
                    .build()
            ).build().graphStore();

        assertEquals(2, graphStore.relationshipTypes().size());
        assertEquals(graphStore.relationshipTypes(), new HashSet<>(asList(
            rel1,
            rel2
        )));

        assertTrue(graphStore.hasRelationshipProperty(rel1, prop1));
        assertFalse(graphStore.hasRelationshipProperty(rel1, prop2));

        assertTrue(graphStore.hasRelationshipProperty(rel2, prop2));
        assertFalse(graphStore.hasRelationshipProperty(rel2, prop1));
    }


    @AllGraphStoreFactoryTypesTest
    void multipleProperties(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 43, p2: 1338, p3: 10}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339, p3: 10}]->(b)" +
            ", (b)-[:REL {p1: 45, p2: 1340, p3: 10}]->(c)" +
            ", (b)-[:REL {p1: 46, p2: 1341, p3: 10}]->(d)"
        );

        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", DefaultValue.of(1.0), Aggregation.NONE),
                PropertyMapping.of("agg2", "p2", DefaultValue.of(2.0), Aggregation.NONE),
                PropertyMapping.of("agg3", "p3", DefaultValue.of(2.0), Aggregation.NONE)
            )
            .graphStore();

        Graph p1Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg1"));
        Graph expectedP1Graph = fromGdl(
            "(a)-[{w: 42}]->(a)" +
            "(a)-[{w: 43}]->(a)" +
            "(a)-[{w: 44}]->(b)" +
            "(b)-[{w: 45}]->(c)" +
            "(b)-[{w: 46}]->(d)"
        );
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg2"));
        Graph expectedP2Graph = fromGdl(
            "(a)-[{w: 1337}]->(a)" +
            "(a)-[{w: 1338}]->(a)" +
            "(a)-[{w: 1339}]->(b)" +
            "(b)-[{w: 1340}]->(c)" +
            "(b)-[{w: 1341}]->(d)"
        );
        assertGraphEquals(expectedP2Graph, p2Graph);

        Graph p3Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg3"));
        Graph expectedP3Graph = fromGdl(
            "(a)-[{w: 2}]->(a)" +
            "(a)-[{w: 10}]->(a)" +
            "(a)-[{w: 10}]->(b)" +
            "(b)-[{w: 10}]->(c)" +
            "(b)-[{w: 10}]->(d)"
        );
        assertGraphEquals(expectedP3Graph, p3Graph);
    }

    @AllGraphStoreFactoryTypesTest
    void multiplePropertiesWithDefaultValues(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (a)-[:REL]->(a)" +
            ", (a)-[:REL {p1: 39}]->(a)" +
            ", (a)-[:REL {p1: 51}]->(a)" +
            ", (b)-[:REL {p1: 45}]->(b)" +
            ", (b)-[:REL]->(b)"
        );
        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", DefaultValue.of(1.0), MIN),
                PropertyMapping.of("agg2", "p1", DefaultValue.of(50.0), MAX),
                PropertyMapping.of("agg3", "p1", DefaultValue.of(3.0), SUM)
            )
            .graphStore();

        Graph p1Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg1"));
        Graph expectedP1Graph = fromGdl(
            "(a)-[{w: 1.0d}]->(a)" +
            "(b)-[{w: 1.0d}]->(b)"
        );
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg2"));
        Graph expectedP2Graph = fromGdl(
            "(a)-[{w: 51.0d}]->(a)" +
            "(b)-[{w: 50.0d}]->(b)"
        );
        assertGraphEquals(expectedP2Graph, p2Graph);

        Graph p3Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg3"));
        Graph expectedP3Graph = fromGdl(
            "(a)-[{w: 93.0d}]->(a)" +
            "(b)-[{w: 48.0d}]->(b)"
        );
        assertGraphEquals(expectedP3Graph, p3Graph);
    }

    @AllGraphStoreFactoryTypesTest
    void multiplePropertiesWithIncompatibleAggregations(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node) " +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339}]->(b)" +
            ", (b)-[:REL {p1: 45, p2: 1340}]->(c)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(d)"
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            TestGraphLoaderFactory.graphLoader(db, factoryType)
                .withRelationshipProperties(
                    PropertyMapping.of("p1", "p1", DefaultValue.of(1.0), Aggregation.NONE),
                    PropertyMapping.of("p2", "p2", DefaultValue.of(2.0), SUM)
                )
                .graphStore()
        );

        assertThat(
            ex.getMessage(),
            containsString(
                "Conflicting relationship property aggregations, it is not allowed to mix `NONE` with aggregations.")
        );
    }

    @AllGraphStoreFactoryTypesTest
    void singlePropertyWithAggregations(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (a)-[:REL {p1: 43}]->(a)" +
            ", (a)-[:REL {p1: 42}]->(a)" +
            ", (a)-[:REL {p1: 44}]->(a)" +
            ", (b)-[:REL {p1: 45}]->(b)" +
            ", (b)-[:REL {p1: 46}]->(b)"
        );

        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipProperties(
                PropertyMapping.of("agg1", "p1", DefaultValue.of(1.0), MAX),
                PropertyMapping.of("agg2", "p1", DefaultValue.of(2.0), MIN)
            )
            .graphStore();

        Graph p1Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg1"));
        Graph expectedP1Graph = fromGdl(
            "(a)-[{w: 44.0d}]->(a)" +
            "(b)-[{w: 46.0d}]->(b)"
        );
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg2"));
        Graph expectedP2Graph = fromGdl(
            "(a)-[{w: 42.0d}]->(a)" +
            "(b)-[{w: 45.0d}]->(b)"
        );
        assertGraphEquals(expectedP2Graph, p2Graph);
    }

    static Stream<Arguments> globalAndLocalAggregationsArguments() {
        return Stream.of(
            Arguments.of(MAX, DEFAULT, DEFAULT, 44, 46, 1339, 1341),
            Arguments.of(MIN, DEFAULT, MAX, 42, 45, 1339, 1341),
            Arguments.of(MIN, MAX, SUM, 44, 46, 4014, 2681)
        );
    }

    static Stream<Arguments> localAggregationArguments() {
        return Stream.of(
            Arguments.of(MIN, 42, 45, 1337, 1340),
            Arguments.of(MAX, 44, 46, 1339, 1341),
            Arguments.of(SUM, 129, 91, 4014, 2681)
        );
    }

    @ParameterizedTest
    @MethodSource("globalAndLocalAggregationsArguments")
    void multiplePropertiesWithGlobalAndLocalAggregations(
        Aggregation globalAggregation,
        Aggregation localAggregation1,
        Aggregation localAggregation2,
        double expectedNodeAP1,
        double expectedNodeBP1,
        double expectedNodeAP2,
        double expectedNodeBP2
    ) {
        clearDb();
        runQuery("" +
                     "CREATE (a:Node),(b:Node),(c:Node),(d:Node) " +
                     "CREATE" +
                     " (a)-[:REL {p1: 42, p2: 1337}]->(a)," +
                     " (a)-[:REL {p1: 43, p2: 1338}]->(a)," +
                     " (a)-[:REL {p1: 44, p2: 1339}]->(a)," +
                     " (b)-[:REL {p1: 45, p2: 1340}]->(b)," +
                     " (b)-[:REL {p1: 46, p2: 1341}]->(b)");

        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, NATIVE)
            .withDefaultAggregation(globalAggregation)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", DefaultValue.of(1.0), localAggregation1),
                PropertyMapping.of("p2", "p2", DefaultValue.of(2.0), localAggregation2)
            )
            .graphStore();

        Graph p1Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("p1"));
        Graph expectedP1Graph = fromGdl(formatWithLocale(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)" +
            "(c), (d)",
            expectedNodeAP1,
            expectedNodeBP1
        ));
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("p2"));
        Graph expectedP2Graph = fromGdl(formatWithLocale(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)" +
            "(c), (d)",
            expectedNodeAP2,
            expectedNodeBP2
        ));
        assertGraphEquals(expectedP2Graph, p2Graph);
    }

    @Test
    void multipleTypesWithSameProperty() {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (a)-[:REL_1 {p1: 43}]->(a)" +
            ", (a)-[:REL_1 {p1: 84}]->(a)" +
            ", (a)-[:REL_2 {p1: 42}]->(a)" +
            ", (a)-[:REL_3 {p1: 44}]->(a)"
        );

        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, NATIVE)
            .withRelationshipTypes("REL_1", "REL_2", "REL_3")
            .withDefaultAggregation(MAX)
            .withRelationshipProperties(
                PropertyMapping.of("agg", "p1", DefaultValue.of(1.0), MAX)
            )
            .graphStore();

        Graph graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("agg"));
        assertEquals(3L, graph.relationshipCount());
        Graph expectedGraph = fromGdl(
            "(a)-[:REL_2 {w: 42.0d}]->(a)" +
            "(a)-[:REL_3 {w: 44.0d}]->(a)" +
            "(a)-[:REL_1 {w: 84.0d}]->(a)"
        );
        assertGraphEquals(expectedGraph, graph);
    }

    @ParameterizedTest
    @MethodSource("localAggregationArguments")
    void multiplePropertiesWithAggregation(
        Aggregation aggregation,
        double expectedNodeAP1,
        double expectedNodeBP1,
        double expectedNodeAP2,
        double expectedNodeBP2
    ) {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (a)-[:REL {p1: 44, p2: 1339}]->(a)" +
            ", (b)-[:REL {p1: 45, p2: 1340}]->(b)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(b)"
        );
        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, NATIVE)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", DefaultValue.of(1.0), aggregation),
                PropertyMapping.of("p2", "p2", DefaultValue.of(2.0), aggregation)
            )
            .graphStore();

        Graph p1Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("p1"));
        Graph expectedP1Graph = fromGdl(formatWithLocale(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)",
            expectedNodeAP1,
            expectedNodeBP1
        ));
        assertGraphEquals(expectedP1Graph, p1Graph);

        Graph p2Graph = graphs.getGraph(graphs.relationshipTypes(), Optional.of("p2"));
        Graph expectedP2Graph = fromGdl(formatWithLocale(
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)",
            expectedNodeAP2,
            expectedNodeBP2
        ));
        assertGraphEquals(expectedP2Graph, p2Graph);
    }

    @AllGraphStoreFactoryTypesTest
    void multiplePropertiesWithAggregation_SINGLE(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        runQuery(
            "CREATE" +
            "  (a:Node)" +
            ", (b:Node) " +
            ", (a)-[:REL {p1: 43, p2: 1338}]->(a)" +
            ", (a)-[:REL {p1: 42, p2: 1337}]->(a)" +
            ", (b)-[:REL {p1: 46, p2: 1341}]->(b)"
        );
        GraphStore graphStore = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipProperties(
                PropertyMapping.of("p1", "p1", DefaultValue.of(1.0), SINGLE),
                PropertyMapping.of("p2", "p2", DefaultValue.of(2.0), SINGLE)
            )
            .graphStore();

        String expectedGraphTemplate =
            "(a)-[{w: %fd}]->(a)" +
            "(b)-[{w: %fd}]->(b)";

        Graph p1Graph = graphStore.getGraph(graphStore.relationshipTypes(), Optional.of("p1"));
        Graph expectedP1GraphOption1 = fromGdl(formatWithLocale(expectedGraphTemplate, 43D, 46D));
        Graph expectedP1GraphOption2 = fromGdl(formatWithLocale(expectedGraphTemplate, 42D, 46D));
        assertGraphEquals(Arrays.asList(expectedP1GraphOption1, expectedP1GraphOption2), p1Graph);

        Graph p2Graph = graphStore.getGraph(graphStore.relationshipTypes(), Optional.of("p2"));
        Graph expectedP2GraphOption1 = fromGdl(formatWithLocale(expectedGraphTemplate, 1338D, 1341D));
        Graph expectedP2GraphOption2 = fromGdl(formatWithLocale(expectedGraphTemplate, 1337D, 1341D));
        assertGraphEquals(Arrays.asList(expectedP2GraphOption1, expectedP2GraphOption2), p2Graph);
    }

    @AllGraphStoreFactoryTypesTest
    void graphsStoreGivesCorrectElementCounts(GraphFactoryTestSupport.FactoryType factoryType) {
        GraphStore graphStore = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("REL1", "REL2", "REL3")
            .graphStore();

        Graph rel1Graph = graphStore.getGraph(RelationshipType.of("REL1"));
        Graph unionGraph = graphStore.getUnion();

        long[] expectedCounts = new long[4];
        runInTransaction(db, tx -> {
            expectedCounts[0] = tx.getAllNodes().stream().count();
            expectedCounts[1] = tx.getAllRelationships().stream().count();
            // The graphs share the node mapping, so we expect the node count for a subgraph
            // to be equal to the node Count for the entire Neo4j graph
            expectedCounts[2] = tx.getAllNodes().stream().count();
            expectedCounts[3] = tx.getAllRelationships()
                .stream()
                .filter(r -> r.isType(org.neo4j.graphdb.RelationshipType.withName("REL1")))
                .count();
        });
        long unionGraphExpectedNodeCount = expectedCounts[0];
        long unionGraphExpectedRelCount = expectedCounts[1];
        long rel1GraphExpectedNodeCount = expectedCounts[2];
        long rel1GraphExpectedRelCount = expectedCounts[3];

        assertEquals(unionGraphExpectedNodeCount, unionGraph.nodeCount());
        assertEquals(unionGraphExpectedRelCount, unionGraph.relationshipCount());
        assertEquals(rel1GraphExpectedNodeCount, rel1Graph.nodeCount());
        assertEquals(rel1GraphExpectedRelCount, rel1Graph.relationshipCount());
    }
}
