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

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.GraphDbApi;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.AdjacencyList;
import org.neo4j.graphalgo.core.huge.AdjacencyOffsets;
import org.neo4j.graphalgo.core.huge.HugeGraph;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;
import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class GraphStoreTest {

    public static final ElementIdentifier LABEL_A = ElementIdentifier.of("A");
    public static final ElementIdentifier LABEL_B = ElementIdentifier.of("B");
    private GraphDbApi db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, " CREATE (a:A {nodeProperty: 33})" +
                     " CREATE (b:B {nodeProperty: 42})" +
                     " CREATE (c:Ignore)" +
                     " CREATE (a)-[:T1 {property1: 42, property2: 1337}]->(b)" +
                     " CREATE (a)-[:T2 {property1: 43}]->(b)" +
                     " CREATE (a)-[:T3 {property2: 1338}]->(b)" +
                     " CREATE (a)-[:T1 {property1: 33}]->(c)" +
                     " CREATE (c)-[:T1 {property1: 33}]->(a)" +
                     " CREATE (b)-[:T1 {property1: 33}]->(c)" +
                     " CREATE (c)-[:T1 {property1: 33}]->(b)");
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validFilterParameters")
    void testFilteringGraphsByRelationships(String desc, List<String> relTypes, Optional<String> relProperty, String expectedGraph) {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .graphName("myGraph")
            .nodeProjections(nodeProjections(false))
            .relationshipProjections(relationshipProjections())
            .build();

        GraphStore graphStore = graphLoader.graphStore(NativeFactory.class);

        Graph filteredGraph =graphStore.getGraph(relTypes, relProperty);

        assertGraphEquals(fromGdl(expectedGraph), filteredGraph);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validFilterParameters")
    void testFilteringGraphsByNodeLabels(String desc, List<String> relTypes, Optional<String> relProperty, String expectedGraph) {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .graphName("myGraph")
            .nodeProjections(nodeProjections(true))
            .relationshipProjections(relationshipProjections())
            .build();

        GraphStore graphStore = graphLoader.graphStore(NativeFactory.class);

        Graph filteredGraph = graphStore.getGraph(Arrays.asList(LABEL_A, LABEL_B), relTypes, relProperty, 1);

        assertGraphEquals(fromGdl(expectedGraph), filteredGraph);
    }

    @Test
    void testFilterNodesWithAllProjectionIncluded() {
        GraphLoader graphLoader = new StoreLoaderBuilder()
            .api(db)
            .graphName("myGraph")
            .putNodeProjectionsWithIdentifier("A", NodeProjection.of("A", PropertyMappings.of()))
            .putNodeProjectionsWithIdentifier("All", NodeProjection.all())
            .loadAnyRelationshipType()
            .build();

        GraphStore graphStore = graphLoader.graphStore(NativeFactory.class);

        Graph filteredAGraph = graphStore.getGraph(
            Collections.singletonList(LABEL_A),
            Collections.singletonList("*"),
            Optional.empty(),
            1
        );

        assertGraphEquals(fromGdl("(a)"), filteredAGraph);

        Graph filteredAllGraph = graphStore.getGraph(
            Collections.singletonList(ElementIdentifier.of("All")),
            Collections.singletonList("*"),
            Optional.empty(),
            1
        );

        Graph nonFilteredGraph = graphStore
            .getGraph(Collections.singletonList(PROJECT_ALL), Collections.singletonList("*"), Optional.empty(), 1);

        assertGraphEquals(filteredAllGraph, nonFilteredGraph);
    }

    @NotNull
    private List<NodeProjection> nodeProjections(boolean includeIgnore) {
        NodeProjection aMapping = NodeProjection.builder()
            .label("A")
            .properties(PropertyMappings.of(PropertyMapping.of("nodeProperty", -1D)))
            .build();
        NodeProjection bMapping = NodeProjection.builder()
            .label("B")
            .properties(PropertyMappings.of(PropertyMapping.of("nodeProperty", -1D)))
            .build();
        NodeProjection ignoreMapping = NodeProjection.of("Ignore");
        return includeIgnore ? Arrays.asList(aMapping, bMapping, ignoreMapping) : Arrays.asList(aMapping, bMapping);
    }

    @NotNull
    private List<RelationshipProjection> relationshipProjections() {
        RelationshipProjection t1Mapping = RelationshipProjection.builder()
            .type("T1")
            .orientation(Orientation.NATURAL)
            .aggregation(Aggregation.NONE)
            .properties(
                PropertyMappings.builder()
                    .addMapping("property1", "property1", 42D, Aggregation.NONE)
                    .addMapping("property2", "property2", 1337D, Aggregation.NONE)
                    .build()
            ).build();

        RelationshipProjection t2Mapping = RelationshipProjection.builder()
            .type("T2")
            .orientation(Orientation.NATURAL)
            .aggregation(Aggregation.NONE)
            .properties(
                PropertyMappings.builder()
                    .addMapping("property1", "property1", 42D, Aggregation.NONE)
                    .build()
            ).build();

        RelationshipProjection t3Mapping = RelationshipProjection.builder()
            .type("T3")
            .orientation(Orientation.NATURAL)
            .aggregation(Aggregation.NONE)
            .properties(
                PropertyMappings.builder()
                    .addMapping("property2", "property2", 42D, Aggregation.NONE)
                    .build()
            ).build();

        return Arrays.asList(t1Mapping, t2Mapping, t3Mapping);
    }

    static Stream<Arguments> validFilterParameters() {
        return Stream.of(
            Arguments.of(
                "filterByRelationshipType",
                singletonList("T1"),
                Optional.empty(),
                "(a {nodeProperty: 33}), (b {nodeProperty: 42}), (a)-[T1]->(b)"
            ),
            Arguments.of(
                "filterByMultipleRelationshipTypes",
                Arrays.asList("T1", "T2"),
                Optional.empty(),
                "(a {nodeProperty: 33}), (b {nodeProperty: 42}), (a)-[T1]->(b), (a)-[T2]->(b)"
            ),
            Arguments.of(
                "filterByAnyRelationshipType",
                singletonList("*"),
                Optional.empty(),
                "(a {nodeProperty: 33}), (b {nodeProperty: 42}), (a)-[T1]->(b), (a)-[T2]->(b), (a)-[T3]->(b)"
            ),
            Arguments.of(
                "filterByRelationshipProperty",
                Arrays.asList("T1", "T2"),
                Optional.of("property1"),
                "(a {nodeProperty: 33}), (b {nodeProperty: 42}), (a)-[T1 {property1: 42}]->(b), (a)-[T2 {property1: 43}]->(b)"
            ),
            Arguments.of(
                "filterByRelationshipTypeAndProperty",
                singletonList("T1"),
                Optional.of("property1"),
                "(a {nodeProperty: 33}), (b {nodeProperty: 42}), (a)-[T1 {property1: 42}]->(b)"
            ),
            /*
              As our graph loader is not capable of loading different relationship properties for different types
              it will still load property1 for T3.
              It seems that the default values it uses is taken from one of the other property mappings
              This test should be adapted once the loader is capable of loading the correct projections.
             */
            Arguments.of(
                "includeRelatiionshipTypesThatDoNotHaveTheProperty",
                singletonList("*"),
                Optional.of("property1"),
                "(a {nodeProperty: 33}), (b {nodeProperty: 42}), (a)-[T1 {property1: 42}]->(b), (a)-[T2 {property1: 43}]->(b), (a)-[T3 {property1: 42.0}]->(b)"
            )
        );
    }

    @Test
    void testModificationDate() throws InterruptedException {
        runQuery("CREATE (a)-[:REL]->(b)");

        GraphStore graphStore = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .build()
            .graphStore(NativeFactory.class);

        // add node properties
        LocalDateTime initialTime = graphStore.modificationTime();
        Thread.sleep(42);
        graphStore.addNodeProperty("foo", new NullPropertyMap(42.0));
        LocalDateTime nodePropertyTime = graphStore.modificationTime();

        // add relationships
        HugeGraph.Relationships relationships = HugeGraph.Relationships.of(
            0L,
            Orientation.NATURAL,
            new AdjacencyList(new byte[0][0]),
            AdjacencyOffsets.of(new long[0]),
            null,
            null,
            42.0
        );
        Thread.sleep(42);
        graphStore.addRelationshipType("BAR", Optional.empty(), relationships);
        LocalDateTime relationshipTime = graphStore.modificationTime();

        assertTrue(initialTime.isBefore(nodePropertyTime), "Node property update did not change modificationTime");
        assertTrue(nodePropertyTime.isBefore(relationshipTime), "Relationship update did not change modificationTime");
    }

    @Test
    void testRemoveNodeProperty() {
        runQuery("CREATE (a {nodeProp: 42})-[:REL]->(b {nodeProp: 23})");

        GraphStore graphStore = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .addNodeProperty(PropertyMapping.of("nodeProp", 0D))
            .loadAnyRelationshipType()
            .build()
            .graphStore(NativeFactory.class);

        assertTrue(graphStore.hasNodeProperty("nodeProp"));
        graphStore.removeNodeProperty("nodeProp");
        assertFalse(graphStore.hasNodeProperty("nodeProp"));
    }

}
