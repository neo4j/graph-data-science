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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.ImmutableModernGraphLoader;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.QueryRunner.runQuery;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;

class GraphCatalogTest {

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(db, " CREATE (a:A)" +
                     " CREATE (b:B)" +
                     " CREATE (a)-[:T1 {property1: 42, property2: 1337}]->(b)" +
                     " CREATE (a)-[:T2 {property1: 43}]->(b)" +
                     " CREATE (a)-[:T3 {property2: 1338}]->(b)");
    }

    @AfterEach
    void teardown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validFilterParameters")
    void testFilteringGraphs(String desc, List<String> relTypes, Optional<String> relProperty, String expectedGraph) {
        RelationshipProjection t1Mapping = RelationshipProjection.builder()
            .type("T1")
            .projection(Projection.NATURAL)
            .aggregation(Aggregation.NONE)
            .properties(
                PropertyMappings.builder()
                    .addMapping("property1", "property1", 42D, Aggregation.NONE)
                    .addMapping("property2", "property2", 1337D, Aggregation.NONE)
                    .build()
            ).build();

        RelationshipProjection t2Mapping = RelationshipProjection.builder()
            .type("T2")
            .projection(Projection.NATURAL)
            .aggregation(Aggregation.NONE)
            .properties(
                PropertyMappings.builder()
                    .addMapping("property1", "property1", 42D, Aggregation.NONE)
                    .build()
            ).build();

        RelationshipProjection t3Mapping = RelationshipProjection.builder()
            .type("T3")
            .projection(Projection.NATURAL)
            .aggregation(Aggregation.NONE)
            .properties(
                PropertyMappings.builder()
                    .addMapping("property2", "property2", 42D, Aggregation.NONE)
                    .build()
            ).build();


        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .username("")
            .graphName("myGraph")
            .nodeProjection(NodeProjections.empty())
            .relationshipProjection(
                RelationshipProjections.builder()
                    .putProjection(ElementIdentifier.of("T1"), t1Mapping)
                    .putProjection(ElementIdentifier.of("T2"), t2Mapping)
                    .putProjection(ElementIdentifier.of("T3"), t3Mapping)
                    .build()
            ).build();

        GraphsByRelationshipType importedGraphs = ImmutableModernGraphLoader
            .builder()
            .api(db)
            .username("")
            .log(new TestLog())
            .createConfig(graphCreateConfig)
            .build()
            .graphs(HugeGraphFactory.class);

        GraphCatalog.set(graphCreateConfig, importedGraphs);

        Graph filteredGraph = GraphCatalog.get("", "myGraph").graph().getGraphProjection(relTypes, relProperty);

        assertGraphEquals(
            fromGdl(
                expectedGraph),
            filteredGraph
        );
    }

    static Stream<Arguments> validFilterParameters() {
        return Stream.of(
            Arguments.of(
                "filterByRelationshipType",
                Collections.singletonList("T1"),
                Optional.empty(),
                "(a), (b), (a)-[T1]->(b)"
            ),
            Arguments.of(
                "filterByMultipleRelationshipTypes",
                Arrays.asList("T1", "T2"),
                Optional.empty(),
                "(a), (b), (a)-[T1]->(b), (a)-[T2]->(b)"
            ),
            Arguments.of(
                "filterByAnyRelationshipType",
                Collections.singletonList("*"),
                Optional.empty(),
                "(a), (b), (a)-[T1]->(b), (a)-[T2]->(b), (a)-[T3]->(b)"
            ),
            Arguments.of(
                "filterByRelationshipProperty",
                Arrays.asList("T1", "T2"),
                Optional.of("property1"),
                "(a), (b), (a)-[T1 {property1: 42}]->(b), (a)-[T2 {property1: 43}]->(b)"
            ),
            Arguments.of(
                "filterByRelationshipTypeAndProperty",
                Collections.singletonList("T1"),
                Optional.of("property1"),
                "(a), (b), (a)-[T1 {property1: 42}]->(b)"
            ),
            /*
              As our graph loader is not capable of loading different relationship properties for different types
              it will still load property1 for T3.
              It seems that the default values it uses is taken from one of the other property mappings
              This test should be adapted once the loader is capable of loading the correct projections.
             */
            Arguments.of(
                "includeRelatiionshipTypesThatDoNotHaveTheProperty",
                Collections.singletonList("*"),
                Optional.of("property1"),
                "(a), (b), (a)-[T1 {property1: 42}]->(b), (a)-[T2 {property1: 43}]->(b), (a)-[T3 {property1: 42.0}]->(b)"
            )
        );
    }
}