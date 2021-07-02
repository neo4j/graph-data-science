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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.fastrp.FastRPMutateProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.core.loading.CatalogRequest;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.degree.DegreeCentralityMutateProc;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class GraphRemoveNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
        ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
        ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
        ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
        ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
        ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    private static Stream<Arguments> nodeProperties() {
        return Stream.of(
            Arguments.of(List.of("nodeProp1"), 3L),
            Arguments.of(List.of("nodeProp2"), 3L),
            Arguments.of(List.of("nodeProp1", "nodeProp2"), 6L)
        );
    }

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphRemoveNodePropertiesProc.class,
            FastRPMutateProc.class,
            DegreeCentralityMutateProc.class
        );
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeProperty("nodeProp1")
            .withNodeProperty("nodeProp2")
            .withAnyRelationshipType()
            .graphCreate(TEST_GRAPH_SAME_PROPERTIES)
            .yields()
        );

        runQuery(GdsCypher.call()
            .withNodeLabel("A", NodeProjection.of(
                "A",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("nodeProp1", 1337),
                    PropertyMapping.of("nodeProp2", 1337)
                )
            ))
            .withNodeLabel("B", NodeProjection.of(
                "B",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("nodeProp1", 1337)
                )
            ))
            .withAnyRelationshipType()
            .graphCreate(TEST_GRAPH_DIFFERENT_PROPERTIES)
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void removeNodeProperties() {
        assertCypherResult(
            "CALL gds.graph.removeNodeProperties($graphName, ['nodeProp1', 'nodeProp2'])",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES),
            List.of(Map.of(
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", List.of("nodeProp1", "nodeProp2"),
                "propertiesRemoved", 12L
            ))
        );
    }

    @ParameterizedTest
    @MethodSource("nodeProperties")
    void removeNodePropertiesForLabel(List<String> nodeProperties, long propertyCount) {
        assertCypherResult(
            "CALL gds.graph.removeNodeProperties($graphName, $nodeProperties, ['A']) YIELD graphName, nodeProperties, propertiesRemoved",
            Map.of("graphName", TEST_GRAPH_DIFFERENT_PROPERTIES, "nodeProperties", nodeProperties),
            List.of(Map.of(
                "graphName", TEST_GRAPH_DIFFERENT_PROPERTIES,
                "nodeProperties", nodeProperties,
                "propertiesRemoved", propertyCount
            ))
        );
    }

    @Test
    void failToRemoveSharedPropertyForLabel() {
        runQuery("CALL gds.degree.mutate($graphName, {mutateProperty: 'score'})", Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES));

        assertError(
            "CALL gds.graph.removeNodeProperties($graphName, ['score'], ['A']) YIELD graphName, nodeProperties, propertiesRemoved",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES),
            "Cannot remove a shared node-property for a subset of node labels. `score` is shared by [A, B]. But only [A] was specified in `nodeLabels`"
        );
    }

    @Test
    void removeNodePropertiesForLabelSubset() {
        assertCypherResult(
            "CALL gds.graph.removeNodeProperties($graphName, ['nodeProp1', 'nodeProp2'])",
            Map.of("graphName", TEST_GRAPH_DIFFERENT_PROPERTIES),
            List.of(Map.of(
                "graphName", TEST_GRAPH_DIFFERENT_PROPERTIES,
                "nodeProperties", List.of("nodeProp1", "nodeProp2"),
                "propertiesRemoved", 6L
            ))
        );
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        assertError(
            "CALL gds.graph.removeNodeProperties($graphName, ['nodeProp1', 'nodeProp2', 'nodeProp3'])",
            Map.of("graphName", TEST_GRAPH_DIFFERENT_PROPERTIES),
            "No node projection with property key(s) ['nodeProp1', 'nodeProp2', 'nodeProp3'] found."
        );
    }
    
    @Test
    void shouldReportRemovalOfFastRPProperties() {
        var fastRPCall = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_SAME_PROPERTIES)
            .algo("fastRP")
            .mutateMode()
            .addParameter("mutateProperty", "fastrp")
            .addParameter("embeddingDimension", 1)
            .yields();

        runQuery(fastRPCall);

        List<String> propertiesToRemove = List.of("fastrp", "nodeProp1", "nodeProp2");

        assertCypherResult(
            "CALL gds.graph.removeNodeProperties( $graphName, $nodeProperties)",
            Map.of("graphName", TEST_GRAPH_SAME_PROPERTIES, "nodeProperties", propertiesToRemove),
            List.of(Map.of(
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", propertiesToRemove,
                "propertiesRemoved", 18L
            ))
        );

        var graphStore = GraphStoreCatalog
            .get(CatalogRequest.of(getUsername(), db.databaseName()), TEST_GRAPH_SAME_PROPERTIES)
            .graphStore();

        assertThat(propertiesToRemove).allMatch(property -> graphStore.nodeLabels().stream().noneMatch(label -> graphStore.hasNodeProperty(label, property)));
    }
}
