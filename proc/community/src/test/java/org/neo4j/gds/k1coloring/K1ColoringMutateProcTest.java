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
package org.neo4j.gds.k1coloring;

import org.assertj.core.api.AssertionsForClassTypes;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.Username;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class K1ColoringMutateProcTest extends BaseProcTest {

    private static final String TEST_USERNAME = Username.EMPTY_USERNAME.username();
    private static final String K1COLORING_GRAPH = "myGraph";
    private static final String MUTATE_PROPERTY = "color";

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        " (a)" +
        ",(b)" +
        ",(c)" +
        ",(d)" +
        ",(a)-[:REL]->(b)" +
        ",(a)-[:REL]->(c)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            K1ColoringMutateProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );
        runQuery(
            GdsCypher.call(K1COLORING_GRAPH)
                .graphProject()
                .loadEverything(Orientation.NATURAL)
                .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private String expectedMutatedGraph() {
        return
            "  (x { color: 0 }) " +
            ", (y { color: 0 }) " +
            ", (z { color: 0 }) " +
            ", (w { color: 1 })-->(y) " +
            ", (w)-->(z) ";
    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.k1coloring","gds.beta.k1coloring"})
    void testMutate(String tieredProcedure) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo(tieredProcedure)
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        var rowCount=runQueryWithRowConsumer(query, row -> {
            assertThat(row.getNumber("preProcessingMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("computeMillis").longValue()).isGreaterThanOrEqualTo(0);
            assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(4);
            assertThat(row.getNumber("colorCount").longValue()).isEqualTo(2);
            assertThat(row.getNumber("ranIterations").longValue()).isLessThan(3);
            assertThat(row.getBoolean("didConverge")).isTrue();

        });

        assertThat(rowCount).isEqualTo(1L);

        var graphStore = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db.databaseName()), K1COLORING_GRAPH).graphStore();

        var mutatedGraph=graphStore.getUnion();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), mutatedGraph);

        var containsMutateProperty =  graphStore.schema().nodeSchema()
            .entries()
            .stream()
            .flatMap(e -> e.properties().entrySet().stream())
            .anyMatch(
                props -> props.getKey().equals(MUTATE_PROPERTY) &&
                    props.getValue().valueType() == ValueType.LONG
            );
        assertThat(containsMutateProperty).isTrue();


    }

    @ParameterizedTest
    @ValueSource(strings = {"gds.k1coloring","gds.beta.k1coloring"})
    void testMutateEstimate(String tieredProcedure) {
        @Language("Cypher")
        String query = GdsCypher.call(K1COLORING_GRAPH).algo(tieredProcedure)
            .mutateEstimation()
            .addParameter("mutateProperty", "color")
            .yields("nodeCount", "bytesMin", "bytesMax", "requiredMemory");

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 4L,
            "bytesMin", 552L,
            "bytesMax", 552L,
            "requiredMemory", "552 Bytes"
        )));
    }

    @Test
    void testWriteBackGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (:B), (a1)-[:REL1]->(a2), (a2)-[:REL2]->(b)");
        String  projectQuery = GdsCypher.call(K1COLORING_GRAPH)
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .yields();
        runQuery(projectQuery);

        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "k1coloring")
            .mutateMode()
            .addParameter("nodeLabels", Collections.singletonList("B"))
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(query);

        String graphWriteQuery =
            "CALL gds.graph.nodeProperties.write(" +
            "   $graph, " +
            "   [$property]" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten";

        runQuery(graphWriteQuery, Map.of("graph", K1COLORING_GRAPH, "property", MUTATE_PROPERTY));

        String checkNeo4jGraphNegativeQuery = formatWithLocale("MATCH (n:A) RETURN n.%s AS property", MUTATE_PROPERTY);

        var rowCountA=runQueryWithRowConsumer(
            checkNeo4jGraphNegativeQuery,
            (resultRow) -> assertNull(resultRow.get("property"))
        );

        assertThat(rowCountA).isEqualTo(2L);

        String checkNeo4jGraphPositiveQuery = formatWithLocale("MATCH (n:B) RETURN n.%s AS property", MUTATE_PROPERTY);

        var rowCountB= runQueryWithRowConsumer(
            checkNeo4jGraphPositiveQuery,
            (resultRow) -> assertNotNull(resultRow.get("property"))
        );

        assertThat(rowCountB).isEqualTo(2L);

    }



    @Test
    void testGraphMutationOnFilteredGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        runQuery("CREATE (a1: A), (a2: A), (b: B), (a1)-[:REL]->(a2)");
        String  projectQuery = GdsCypher.call(K1COLORING_GRAPH)
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .yields();
        runQuery(projectQuery);


        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "k1coloring")
            .mutateMode()
            .addParameter("nodeLabels", Collections.singletonList("A"))
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();

        runQuery(query);

        var mutatedGraph = GraphStoreCatalog.get(TEST_USERNAME, DatabaseId.of(db.databaseName()), K1COLORING_GRAPH).graphStore();

        var expectedProperties = Set.of(MUTATE_PROPERTY);
        assertEquals(expectedProperties, mutatedGraph.nodePropertyKeys(NodeLabel.of("A")));
        assertEquals(Set.of(), mutatedGraph.nodePropertyKeys(NodeLabel.of("B")));
    }

    @Test
    void testMutateFailsOnExistingToken() {
        String query = GdsCypher.call(K1COLORING_GRAPH).algo("gds", "k1coloring")
            .mutateMode()
            .addParameter("mutateProperty", MUTATE_PROPERTY)
            .yields();
        runQuery(query);

        assertThatException().isThrownBy( () -> runQuery(query)).withMessageContaining(formatWithLocale(
        "Node property `%s` already exists in the in-memory graph.",
        MUTATE_PROPERTY
        ));
    }

    @Test
    void testExceptionLogging() {
        List<TestLog> log = new ArrayList<>(1);
        assertThrows(
            NullPointerException.class,
            ()->TestProcedureRunner.applyOnProcedure(db, K1ColoringMutateProc.class,
                procedure -> {
                var computationResult = mock(ComputationResult.class);
                log.add(0, ((TestLog) procedure.log));
                new K1ColoringMutateSpecification().computationResultConsumer().consume(computationResult, procedure.executionContext());
            })
        );

        assertTrue(log.get(0).containsMessage(TestLog.WARN, "Graph mutation failed"));
    }

    @Test
    void testRunOnEmptyGraph() {
        // Create a dummy node with label "X" so that "X" is a valid label to put use for property mappings later

        runQuery("CALL db.createLabel('X')");
        runQuery("MATCH (n) DETACH DELETE n");
        GraphStoreCatalog.removeAllLoadedGraphs();

        String  projectQuery = GdsCypher.call("foo")
            .graphProject().withNodeLabel("X").yields();
        runQuery(projectQuery);

        String query = GdsCypher.call("foo")
            .algo("gds", "k1coloring")
            .mutateMode()
            .addParameter("mutateProperty","foo2")
            .yields();


        var rowCount=runQueryWithRowConsumer(query, row -> {
            AssertionsForClassTypes.assertThat(row.getNumber("preProcessingMillis").longValue()).isNotEqualTo(-1);
            AssertionsForClassTypes.assertThat(row.getNumber("computeMillis").longValue()).isEqualTo(0);
            AssertionsForClassTypes.assertThat(row.getNumber("nodeCount").longValue()).isEqualTo(0);
            AssertionsForClassTypes.assertThat(row.getNumber("colorCount").longValue()).isEqualTo(0);
            AssertionsForClassTypes.assertThat(row.getNumber("ranIterations").longValue()).isEqualTo(0);
            AssertionsForClassTypes.assertThat(row.getBoolean("didConverge")).isFalse();
        });

        AssertionsForClassTypes.assertThat(rowCount).isEqualTo(1L);
    }



}
