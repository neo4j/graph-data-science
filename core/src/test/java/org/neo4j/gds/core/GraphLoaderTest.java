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
import org.neo4j.gds.CypherLoaderBuilder;
import org.neo4j.gds.GraphFactoryTestSupport;
import org.neo4j.gds.GraphFactoryTestSupport.AllGraphStoreFactoryTypesTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.TestGraphLoaderFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.gds.GraphFactoryTestSupport.FactoryType.NATIVE;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.assertTransactionTermination;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.utils.GdsFeatureToggles.SKIP_ORPHANS;
import static org.neo4j.gds.utils.GdsFeatureToggles.USE_PARALLEL_PROPERTY_VALUE_INDEX;
import static org.neo4j.gds.utils.GdsFeatureToggles.USE_PROPERTY_VALUE_INDEX;

class GraphLoaderTest extends BaseTest {

    public static final String DB_CYPHER =
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

    @AllGraphStoreFactoryTypesTest
    void testAnyLabel(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType).withDefaultAggregation(Aggregation.SINGLE).graph();
        assertGraphEquals(fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithLabel(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType).withLabels("Node1").graph();
        assertGraphEquals(fromGdl("(:Node1)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithMultipleLabels(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType).withLabels("Node1", "Node2").graph();
        assertGraphEquals(fromGdl("(a:Node1)-->(b:Node2)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithMultipleLabelsAndProperties(GraphFactoryTestSupport.FactoryType factoryType) {
        PropertyMappings properties = PropertyMappings.of(PropertyMapping.of("prop1", 42L));
        PropertyMappings multipleProperties = PropertyMappings.of(
            PropertyMapping.of("prop1", 42L),
            PropertyMapping.of("prop2", 42L)
        );

        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Node1", "Node2")
            .withNodeProperties(properties)
            .graph();
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1L})-->(b:Node2 {prop1: 42L})"), graph);

        graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Node1", "Node2")
            .withNodeProperties(multipleProperties)
            .graph();
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1L, prop2: 42L})-->(b:Node2 {prop1: 42L, prop2: 2L})"), graph);
    }

    @Test
    void shouldLogProgressWithNativeLoading() {
        var log = Neo4jProxy.testLog();
        new StoreLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .nodeProjectionsWithIdentifier(Map.of("AllNodes", NodeProjection.all()))
            .relationshipProjectionsWithIdentifier(Map.of("AllRels", RelationshipProjection.ALL))
            .nodeProperties(List.of(PropertyMapping.of("prop1", 42L)))
            .log(log)
            .build()
            .graph();

        log.assertContainsMessage(TestLog.DEBUG, "Loading :: Nodes :: Store Scan :: Imported 3 records and 1 properties");
        log.assertContainsMessage(TestLog.DEBUG, "Loading :: Relationships :: Store Scan :: Imported 4 records and 0 properties");
    }

    @Test
    public void shouldTrackProgressWithNativeLoading() {
        TestLog log = Neo4jProxy.testLog();

        new StoreLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .nodeProjectionsWithIdentifier(Map.of("AllNodes", NodeProjection.all()))
            .relationshipProjectionsWithIdentifier(Map.of("AllRels", RelationshipProjection.ALL))
            .nodeProperties(List.of(PropertyMapping.of("prop1", 42L)))
            .log(log)
            .build()
            .graph();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "Loading :: Start",
                "Loading :: Nodes :: Start",
                "Loading :: Nodes :: Store Scan :: Start",
                "Loading :: Nodes :: Store Scan 100%",
                "Loading :: Nodes :: Store Scan :: Finished",
                "Loading :: Nodes :: Finished",
                "Loading :: Relationships :: Start",
                "Loading :: Relationships :: Store Scan :: Start",
                "Loading :: Relationships :: Store Scan 100%",
                "Loading :: Relationships :: Store Scan :: Finished",
                "Loading :: Relationships :: Finished",
                "Loading :: Finished"
            )
            .doesNotContain("Loading :: Nodes :: Property Index Scan :: Start");

        assertThat(log.getMessages(TestLog.DEBUG))
            .extracting(removingThreadId())
            .contains("Loading :: Nodes :: Store Scan :: Start using NodeCursorBasedScanner")
            .contains("Loading :: Relationships :: Store Scan :: Start using RelationshipScanCursorBasedScanner");

        log.assertContainsMessage(TestLog.DEBUG, "Loading :: Nodes :: Store Scan :: Imported 3 records and 1 properties");
        log.assertContainsMessage(TestLog.DEBUG, "Loading :: Relationships :: Store Scan :: Imported 4 records and 0 properties");
    }

    @Test
    public void shouldTrackProgressWithNativeLoadingUsingIndex() {
        TestLog log = Neo4jProxy.testLog();

        USE_PROPERTY_VALUE_INDEX.enableAndRun(() -> testPropertyLoadingWithIndex(NATIVE, log));

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "Loading :: Start",
                "Loading :: Nodes :: Start",
                "Loading :: Nodes :: Store Scan :: Start",
                "Loading :: Nodes :: Store Scan 100%",
                "Loading :: Nodes :: Store Scan :: Finished",
                "Loading :: Nodes :: Property Index Scan :: Start",
                "Loading :: Nodes :: Property Index Scan 100%",
                "Loading :: Nodes :: Property Index Scan :: Finished",
                "Loading :: Nodes :: Finished",
                "Loading :: Relationships :: Start",
                "Loading :: Relationships :: Store Scan :: Start",
                "Loading :: Relationships :: Store Scan 100%",
                "Loading :: Relationships :: Store Scan :: Finished",
                "Loading :: Relationships :: Finished",
                "Loading :: Finished"
            );

        assertThat(log.getMessages(TestLog.DEBUG))
            .extracting(removingThreadId())
            .contains("Loading :: Nodes :: Store Scan :: Start using MultipleNodeLabelIndexBasedScanner")
            .contains("Loading :: Relationships :: Store Scan :: Start using RelationshipScanCursorBasedScanner");

        log.assertContainsMessage(TestLog.DEBUG, "Loading :: Nodes :: Store Scan :: Imported 3 records and 1 properties");
        log.assertContainsMessage(TestLog.DEBUG, "Loading :: Relationships :: Store Scan :: Imported 4 records and 0 properties");
    }

    @Test
    void shouldLogProgressWithCypherLoading() {
        var log = Neo4jProxy.testLog();
        new CypherLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .nodeQuery("MATCH (n) RETURN id(n) AS id, coalesce(n.prop1, 42) AS prop1")
            .relationshipQuery("MATCH (n)-[:REL1|REL2]->(m) RETURN id(n) AS source, id(m) AS target")
            .log(log)
            .build()
            .graph();
        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "Loading :: Start",
                "Loading :: Nodes :: Start",
                "Loading :: Nodes 33%",
                "Loading :: Nodes 66%",
                "Loading :: Nodes 100%",
                "Loading :: Nodes :: Start",
                "Loading :: Nodes :: Finished",
                "Loading :: Relationships :: Start",
                "Loading :: Relationships 25%",
                "Loading :: Relationships 50%",
                "Loading :: Relationships 75%",
                "Loading :: Relationships :: Finished",
                "Loading :: Finished"
            );

        assertThat(log.getMessages(TestLog.DEBUG)).isEmpty();
    }

    @AllGraphStoreFactoryTypesTest
    void testWithSingleLabelAndProperties(GraphFactoryTestSupport.FactoryType factoryType) {
        PropertyMappings properties = PropertyMappings.of(PropertyMapping.of("prop1", 42));
        PropertyMappings multipleProperties = PropertyMappings.of(
            PropertyMapping.of("prop1", 42),
            PropertyMapping.of("prop2", 42)
        );

        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Node1")
            .withNodeProperties(properties)
            .graph();
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1})"), graph);

        graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Node1")
            .withNodeProperties(multipleProperties)
            .graph();
        assertGraphEquals(fromGdl("(a:Node1 {prop1: 1, prop2: 42})"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testAnyRelation(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType).withDefaultAggregation(Aggregation.SINGLE).graph();
        assertGraphEquals(fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithBothWeightedRelationship(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("REL3")
            .withRelationshipProperties(PropertyMapping.of("weight", 1.0))
            .graph();

        assertGraphEquals(fromGdl("(), ()-[:REL3 {w:1337}]->()"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithOutgoingRelationship(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("REL3")
            .graph();
        assertGraphEquals(fromGdl("(), ()-[:REL3]->()"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithNodeProperties(GraphFactoryTestSupport.FactoryType factoryType) {
        PropertyMappings nodePropertyMappings = PropertyMappings.of(
            PropertyMapping.of("prop1", "prop1", 0),
            PropertyMapping.of("prop2", "prop2", 0),
            PropertyMapping.of("prop3", "prop3", 0)
        );

        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withNodeProperties(nodePropertyMappings)
            .withDefaultAggregation(Aggregation.SINGLE)
            .graph();

        Graph expected = fromGdl("(a {prop1: 1, prop2: 0, prop3: 0})" +
                                 "(b {prop1: 0, prop2: 2, prop3: 0})" +
                                 "(c {prop1: 0, prop2: 0, prop3: 3})" +
                                 "(a)-->(b), (a)-->(c), (b)-->(c)");
        assertGraphEquals(expected, graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testWithRelationshipProperty(GraphFactoryTestSupport.FactoryType factoryType) {
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipProperties(PropertyMapping.of("weight", "prop1", 3.14))
            .withDefaultAggregation(Aggregation.SINGLE)
            .graph();
        assertGraphEquals(fromGdl("(a)-[{w: 1}]->(b), (a)-[{w: 3.14D}]->(c), (b)-[{w: 3.14D}]->(c)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testLoadCorrectLabelCombinations(GraphFactoryTestSupport.FactoryType factoryType) {
        runQuery("CREATE (n:Node1:Node2)");
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Node1", "Node2")
            .graph();
        assertGraphEquals(fromGdl("(a:Node1), (b:Node2), (c:Node1:Node2), (a)-->(b)"), graph);
    }

    @Test
    void testLoadNodeWithMultipleLabelsOnPartialLabelMatch() {
        runQuery("CREATE (n:Node1:Node2)");
        Graph graph = TestGraphLoaderFactory.graphLoader(db, NATIVE)
            .withLabels("Node1")
            .graph();
        assertGraphEquals(fromGdl("(a:Node1), (c:Node1)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void testPropertyViaIndex(GraphFactoryTestSupport.FactoryType factoryType) {
        USE_PROPERTY_VALUE_INDEX.enableAndRun(() ->
            testPropertyLoadingWithIndex(factoryType, NullLog.getInstance()));
    }

    @AllGraphStoreFactoryTypesTest
    void testParallelPropertyViaIndex(GraphFactoryTestSupport.FactoryType factoryType) {
        USE_PROPERTY_VALUE_INDEX.enableAndRun(() ->
            USE_PARALLEL_PROPERTY_VALUE_INDEX.enableAndRun(() ->
                testPropertyLoadingWithIndex(factoryType, NullLog.getInstance())));
    }

    private void testPropertyLoadingWithIndex(GraphFactoryTestSupport.FactoryType factoryType, Log log) {
        var indexQueries = List.of(
            "CREATE INDEX prop1 FOR (n:Node1) ON (n.prop1)",
            "CREATE INDEX prop2 FOR (n:Node2) ON (n.prop2)"
        );
        indexQueries.forEach(this::runQuery);

        PropertyMappings nodePropertyMappings = PropertyMappings.of(
            PropertyMapping.of("prop1", "prop1", 41L),
            PropertyMapping.of("prop2", "prop2", 42L),
            PropertyMapping.of("prop3", "prop3", 43L)
        );

        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Node1", "Node2", "Node3")
            .withNodeProperties(nodePropertyMappings)
            .withDefaultAggregation(Aggregation.SINGLE)
            .withLog(log)
            .graph();

        Graph expected = fromGdl("(a:Node1 {prop1: 1, prop2: 42, prop3: 43})" +
                                 "(b:Node2 {prop1: 41, prop2: 2, prop3: 43})" +
                                 "(c:Node3 {prop1: 41, prop2: 42, prop3: 3})" +
                                 "(a)-->(b), (a)-->(c), (b)-->(c)");
        assertGraphEquals(expected, graph);
    }

    @Test
    void testDontSkipOrphanNodesByDefault() {
        runQuery("CREATE (:X),(:Y),(:X),(:Y)-[:Q]->(:Z)");
        Graph graph = TestGraphLoaderFactory
            .graphLoader(db, NATIVE)
            .withLabels("X", "Y", "Z")
            .withRelationshipTypes("Q")
            .graph();
        TestGraph expected = fromGdl("(:X),(:Y),(:X),(:Y)-[:Q]->(:Z)");
        assertGraphEquals(expected, graph);
    }

    @Test
    void testSkipOrphanNodes() {
        SKIP_ORPHANS.enableAndRun(() -> {
            runQuery("CREATE (:X),(:Y),(:X),(:Y)-[:Q]->(:Z)");
            Graph graph = TestGraphLoaderFactory
                .graphLoader(db, NATIVE)
                .withLabels("X", "Y", "Z")
                .withRelationshipTypes("Q")
                .graph();
            TestGraph expected = fromGdl("(:Y)-[:Q]->(:Z)");
            assertGraphEquals(expected, graph);
        });
    }

    @Test
    void stopsImportingWhenTransactionHasBeenTerminated() {
        TerminationFlag terminationFlag = () -> false;
        assertTransactionTermination(
            () -> new StoreLoaderBuilder()
                .databaseService(db)
                .terminationFlag(terminationFlag)
                .build()
                .graph()
        );
    }

    @AllGraphStoreFactoryTypesTest
    void testLoggingActualGraphSize(GraphFactoryTestSupport.FactoryType factoryType) {
        var log = Neo4jProxy.testLog();
        Graph graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withDefaultAggregation(Aggregation.SINGLE)
            .withLog(log)
            .graph();
        assertGraphEquals(fromGdl("(a)-->(b), (a)-->(c), (b)-->(c)"), graph);
        log.containsMessage(TestLog.INFO, "Loading :: Actual memory usage of the loaded graph:");
    }

    @AllGraphStoreFactoryTypesTest
    void testNodePropertyDefaultValue(GraphFactoryTestSupport.FactoryType factoryType) {
        String createQuery = "CREATE" +
                             "  (:Label { weight1: 0.0, weight2: 1.0 })" +
                             ", (:Label { weight2: 1.0 })" +
                             ", (:Label { weight1: 0.0 })";
        runQuery(createQuery);
        var graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withLabels("Label")
            .withNodeProperties(PropertyMappings.of(
                PropertyMapping.of("weight1", 0.0),
                PropertyMapping.of("weight2", 1.0)
            )).graph();

        graph.forEachNode(nodeId -> {
            assertEquals(0.0, graph.nodeProperties("weight1").doubleValue(nodeId));
            assertEquals(1.0, graph.nodeProperties("weight2").doubleValue(nodeId));
            return true;
        });
    }

    @AllGraphStoreFactoryTypesTest
    void shouldFilterRelationshipType(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        String createQuery = "CREATE (a)-[:Foo]->(b)-[:Bar]->(c)-[:Foo]->(d)";
        runQuery(createQuery);

        var graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("Foo")
            .graph();

        assertGraphEquals(fromGdl("(a)-[:Foo]->(b), (c)-[:Foo]->(d)"), graph);
    }

    @AllGraphStoreFactoryTypesTest
    void shouldFilterRelationshipTypeAndApplyDefaultRelationshipProperty(GraphFactoryTestSupport.FactoryType factoryType) {
        clearDb();
        String createQuery = "CREATE (a)-[:Foo { bar: 3.14 }]->(b)-[:Baz { bar: 2.71 }]->(c)-[:Foo]->(d)";
        runQuery(createQuery);

        var graph = TestGraphLoaderFactory.graphLoader(db, factoryType)
            .withRelationshipTypes("Foo")
            .withRelationshipProperties(PropertyMappings.of(
                PropertyMapping.of("bar", 1.61)
            )).graph();

        assertGraphEquals(fromGdl("(a)-[:Foo {bar: 3.14D}]->(b), (c)-[:Foo {bar: 1.61D}]->(d)"), graph);
    }

    @Test
    void shouldLoadSingleNodeLabelViaNativeLoader() {
        var graphStore = new StoreLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .nodeProjectionsWithIdentifier(Map.of("AllNodes", NodeProjection.all()))
            .relationshipProjectionsWithIdentifier(Map.of("AllRels", RelationshipProjection.ALL))
            .nodeProperties(List.of(PropertyMapping.of("prop1", 42L)))
            .build()
            .graphStore();

        assertThat(graphStore.nodeLabels()).containsExactly(NodeLabel.of("AllNodes"));
    }

    @Test
    void shouldInverseIndexRelationships() {
        var relationshipType = RelationshipType.of("Rel");
        var graphStore = new StoreLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .putNodeProjectionsWithIdentifier("Node", NodeProjection.all())
            .putRelationshipProjectionsWithIdentifier(
                relationshipType.name(),
                RelationshipProjection.builder().type("*").indexInverse(true).build()
            )
            .build()
            .graphStore();

        var graph = graphStore.getGraph(relationshipType);
        assertThat(graph.characteristics()).satisfies(c -> assertThat(c.isInverseIndexed()).isTrue());
    }

    static Stream<Arguments> orientationCombinations() {
        return Stream.of(
            Arguments.of(Orientation.NATURAL, Orientation.NATURAL, false),
            Arguments.of(Orientation.UNDIRECTED, Orientation.NATURAL, false),
            Arguments.of(Orientation.NATURAL, Orientation.UNDIRECTED, false),
            Arguments.of(Orientation.UNDIRECTED, Orientation.UNDIRECTED, true)
        );
    }

    @ParameterizedTest
    @MethodSource("orientationCombinations")
    void nativeOrientations(Orientation orientation1, Orientation orientation2, boolean undirectedExpectation) {
        var graphStore = new StoreLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .addNodeLabel("Node1")
            .addRelationshipProjection(RelationshipProjection.of("REL1", orientation1))
            .addRelationshipProjection(RelationshipProjection.of("REL2", orientation2))
            .build()
            .graphStore();

        assertThat(graphStore.schema().relationshipSchema().isUndirected()).isEqualTo(undirectedExpectation);
    }

    @Test
    void cypherProjectionsAreNeverUndirected() {
        var graphStore = new CypherLoaderBuilder()
            .databaseService(db)
            .graphName("graph")
            .nodeQuery("UNWIND [0, 1] AS id RETURN id")
            .relationshipQuery("RETURN 0 AS source, 1 AS target, 'TEST' AS type")
            .build()
            .graphStore();

        assertFalse(graphStore.schema().relationshipSchema().isUndirected());
    }
}
