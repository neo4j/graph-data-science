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
package org.neo4j.gds.similarity.nodesim;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.catalog.GraphDropProc;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.REVERSE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class NodeSimilarityStreamProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Person {id: 0,  name: 'Alice'})" +
        ", (b:Person {id: 1,  name: 'Bob'})" +
        ", (c:Person {id: 2,  name: 'Charlie'})" +
        ", (d:Person {id: 3,  name: 'Dave'})" +
        ", (i1:Item  {id: 10, name: 'p1'})" +
        ", (i2:Item  {id: 11, name: 'p2'})" +
        ", (i3:Item  {id: 12, name: 'p3'})" +
        ", (i4:Item  {id: 13, name: 'p4'})" +
        ", (a)-[:LIKES]->(i1)" +
        ", (a)-[:LIKES]->(i2)" +
        ", (a)-[:LIKES]->(i3)" +
        ", (b)-[:LIKES]->(i1)" +
        ", (b)-[:LIKES]->(i2)" +
        ", (c)-[:LIKES]->(i3)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            NodeSimilarityStreamProc.class,
            GraphProjectProc.class
        );

        TestSupport.allDirectedProjections().forEach(orientation -> {
            String name = "myGraph" + orientation.name();
            String createQuery = GdsCypher.call(name)
                .graphProject()
                .withAnyLabel()
                .withRelationshipType(
                    "LIKES",
                    RelationshipProjection.builder().type("LIKES").orientation(orientation).build()
                )
                .yields();
            runQuery(createQuery);
        });
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private static final Collection<String> EXPECTED_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING = new HashSet<>();
    private static final Collection<String> EXPECTED_TOP_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_TOP_INCOMING = new HashSet<>();
    private static final Collection<String> EXPECTED_DEGREE_CUTOFF_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_DEGREE_CUTOFF_INCOMING = new HashSet<>();

    private static String resultString(long node1, long node2, double similarity) {
        return formatWithLocale("%d,%d %f%n", node1, node2, similarity);
    }

    static {
        EXPECTED_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(1, 2, 0.0));
        // With mandatory topK, expect results in both directions
        EXPECTED_OUTGOING.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 1, 0.0));

        EXPECTED_TOP_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_TOP_OUTGOING.add(resultString(1, 0, 2 / 3.0));

        EXPECTED_DEGREE_CUTOFF_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_DEGREE_CUTOFF_OUTGOING.add(resultString(1, 0, 2 / 3.0));

        EXPECTED_DEGREE_CUTOFF_INCOMING.add(resultString(4, 5, 3.0 / 3.0));
        EXPECTED_DEGREE_CUTOFF_INCOMING.add(resultString(5, 4, 3.0 / 3.0));
        EXPECTED_DEGREE_CUTOFF_INCOMING.add(resultString(4, 6, 1 / 3.0));
        EXPECTED_DEGREE_CUTOFF_INCOMING.add(resultString(6, 4, 1 / 3.0));
        EXPECTED_DEGREE_CUTOFF_INCOMING.add(resultString(5, 6, 1 / 3.0));
        EXPECTED_DEGREE_CUTOFF_INCOMING.add(resultString(6, 5, 1 / 3.0));

        EXPECTED_INCOMING.add(resultString(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING.add(resultString(4, 6, 1 / 3.0));
        EXPECTED_INCOMING.add(resultString(5, 6, 1 / 3.0));
        // With mandatory topK, expect results in both directions
        EXPECTED_INCOMING.add(resultString(5, 4, 3.0 / 3.0));
        EXPECTED_INCOMING.add(resultString(6, 4, 1 / 3.0));
        EXPECTED_INCOMING.add(resultString(6, 5, 1 / 3.0));

        EXPECTED_TOP_INCOMING.add(resultString(4, 5, 3.0 / 3.0));
        EXPECTED_TOP_INCOMING.add(resultString(5, 4, 3.0 / 3.0));
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("allValidGraphVariationsWithProjections")
    void shouldStreamResults(GdsCypher.QueryBuilder queryBuilder, Orientation orientation, String testName) {
        String query = queryBuilder
            .algo("nodeSimilarity")
            .streamMode()
            .addParameter("similarityCutoff", 0.0)
            .yields("node1", "node2", "similarity");

        Collection<String> result = new HashSet<>();
        runQueryWithRowConsumer(query, row -> {
            long node1 = row.getNumber("node1").longValue();
            long node2 = row.getNumber("node2").longValue();
            double similarity = row.getNumber("similarity").doubleValue();
            result.add(resultString(node1, node2, similarity));
        });

        assertEquals(
            orientation == REVERSE
                ? EXPECTED_INCOMING
                : EXPECTED_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("allValidGraphVariationsWithProjections")
    void shouldStreamTopResults(GdsCypher.QueryBuilder queryBuilder, Orientation orientation, String testName) {
        int topN = 2;
        String query = queryBuilder
            .algo("nodeSimilarity")
            .streamMode()
            .addParameter("topN", topN)
            .yields("node1", "node2", "similarity");

        Collection<String> result = new HashSet<>();
        runQueryWithRowConsumer(query, row -> {
            long node1 = row.getNumber("node1").longValue();
            long node2 = row.getNumber("node2").longValue();
            double similarity = row.getNumber("similarity").doubleValue();
            result.add(resultString(node1, node2, similarity));
        });

        assertEquals(
            orientation == REVERSE
                ? EXPECTED_TOP_INCOMING
                : EXPECTED_TOP_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("allValidGraphVariationsWithProjections")
    void shouldStreamWithDegreeCutOff(GdsCypher.QueryBuilder queryBuilder, Orientation orientation, String testName) {
        int degreeCutoff = 2;

        Collection<String> result = new HashSet<>();
        String query = queryBuilder
            .algo("nodeSimilarity")
            .streamMode()
            .addParameter("degreeCutoff", degreeCutoff)
            .yields("node1", "node2", "similarity");

        runQueryWithRowConsumer(query, row -> {
            long node1 = row.getNumber("node1").longValue();
            long node2 = row.getNumber("node2").longValue();
            double similarity = row.getNumber("similarity").doubleValue();
            result.add(resultString(node1, node2, similarity));
        });

        assertEquals(
            orientation == REVERSE
                ? EXPECTED_DEGREE_CUTOFF_INCOMING
                : EXPECTED_DEGREE_CUTOFF_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("allValidGraphVariationsWithProjections")
    void shouldIgnoreParallelEdges(GdsCypher.QueryBuilder queryBuilder, Orientation orientation, String testName) {
        // Add parallel edges
        runQuery("" +
                 " MATCH (person {name: 'Alice'})" +
                 " MATCH (thing {name: 'p1'})" +
                 " CREATE (person)-[:LIKES]->(thing)"
        );
        runQuery("" +
                 " MATCH (person {name: 'Charlie'})" +
                 " MATCH (thing {name: 'p3'})" +
                 " CREATE (person)-[:LIKES]->(thing)" +
                 " CREATE (person)-[:LIKES]->(thing)" +
                 " CREATE (person)-[:LIKES]->(thing)"
        );

        String query = queryBuilder
            .algo("nodeSimilarity")
            .streamMode()
            .addParameter("similarityCutoff", 0.0)
            .yields("node1", "node2", "similarity");

        Collection<String> result = new HashSet<>();
        runQueryWithRowConsumer(query, row -> {
            long node1 = row.getNumber("node1").longValue();
            long node2 = row.getNumber("node2").longValue();
            double similarity = row.getNumber("similarity").doubleValue();
            result.add(resultString(node1, node2, similarity));
        });

        assertEquals(
            orientation == REVERSE
                ? EXPECTED_INCOMING
                : EXPECTED_OUTGOING,
            result
        );
    }


    static Stream<Arguments> allGraphVariations() {
        return graphVariationForProjection(NATURAL).map(args -> arguments(args.get()[0], args.get()[2]));
    }

    static Stream<Arguments> allValidGraphVariationsWithProjections() {
        return TestSupport.allDirectedProjections().flatMap(NodeSimilarityStreamProcTest::graphVariationForProjection);
    }

    private static Stream<Arguments> graphVariationForProjection(Orientation orientation) {
        String name = "myGraph" + orientation.name();
        return Stream.of(
            arguments(
                GdsCypher.call(name),
                orientation,
                "explicit graph - " + orientation
            )
        );
    }


    @Nested
    @TestInstance(value = TestInstance.Lifecycle.PER_METHOD)
    class NonConsecutiveIds extends BaseTest {

        @Neo4jGraph
        private static final String DB_CYPHER_NON_CONSECUTIVE =
            "CREATE (:IncrementIdSpace)" + DB_CYPHER;

        @ParameterizedTest(name = "{1}")
        @MethodSource("org.neo4j.gds.similarity.nodesim.NodeSimilarityStreamProcTest#allGraphVariations")
        void shouldDealWithAnyIdSpace(GdsCypher.QueryBuilder queryBuilder, String testName) throws Exception {
            String graphCreate =
                "CALL gds.graph.project(" +
                "    'myGraphNATURAL'," +
                "    ['Person', 'Item']," +
                "    'LIKES'" +
                ")";

            int idOffset = 1;
            long deletedNodes = 0;
            registerProcedures(GraphDropProc.class);
            runQuery("CALL gds.graph.drop('myGraphNATURAL')");
            runQuery(graphCreate);

            Set<String> expected = Set.of(
                resultString(idOffset + deletedNodes + 0, idOffset + deletedNodes + 1, 2 / 3.0),
                resultString(idOffset + deletedNodes + 0, idOffset + deletedNodes + 2, 1 / 3.0),
                resultString(idOffset + deletedNodes + 1, idOffset + deletedNodes + 2, 0.0),
                resultString(idOffset + deletedNodes + 1, idOffset + deletedNodes + 0, 2 / 3.0),
                resultString(idOffset + deletedNodes + 2, idOffset + deletedNodes + 0, 1 / 3.0),
                resultString(idOffset + deletedNodes + 2, idOffset + deletedNodes + 1, 0.0)
            );

            String query = queryBuilder
                .algo("nodeSimilarity")
                .streamMode()
                .addParameter("similarityCutoff", 0.0)
                .yields("node1", "node2", "similarity");

            Collection<String> result = new HashSet<>();
            runQueryWithRowConsumer(query, row -> {
                    long node1 = row.getNumber("node1").longValue();
                    long node2 = row.getNumber("node2").longValue();
                    double similarity = row.getNumber("similarity").doubleValue();
                    result.add(resultString(node1, node2, similarity));
                }
            );

            assertEquals(expected, result);
        }
    }


    @ParameterizedTest
    @ValueSource(ints = {0, 10})
    void shouldStreamWithFilteredNodes(int topN) {
        runQuery("MATCH (n) DETACH DELETE n");
        String graphCreateQuery =
            "CREATE (alice:Person)" +
            ", (carol:Person)" +
            ", (eve:Person)" +
            ", (dave:Foo)" +
            ", (bob:Foo)" +
            ", (a:Bar)" +
            ", (dave)-[:KNOWS]->(a)" +
            ", (bob)-[:KNOWS]->(a)";
        runQuery(graphCreateQuery);

        String createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("Person")
            .withNodeLabel("Foo")
            .withNodeLabel("Bar")
            .withAnyRelationshipType()
            .yields();
        runQuery(createQuery);

        String algoQuery = GdsCypher.call("graph")
            .algo("gds.nodeSimilarity")
            .streamMode()
            .addParameter("nodeLabels", List.of("Foo", "Bar"))
            .addParameter("topN", topN)
            .yields("node1", "node2", "similarity");

        var rowCount = runQueryWithRowConsumer(algoQuery, row -> {
            assertThat(row.getNumber("node1")).isIn(11L, 12L);
            assertThat(row.getNumber("node2")).isIn(11L, 12L);
            assertThat(row.getNumber("similarity")).isEqualTo(1.0);
        });

        assertThat(rowCount).isEqualTo(2l);
    }
}
