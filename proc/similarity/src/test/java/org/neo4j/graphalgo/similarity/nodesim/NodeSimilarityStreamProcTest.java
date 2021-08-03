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
package org.neo4j.graphalgo.similarity.nodesim;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.catalog.GraphDropProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityStreamConfig;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.extension.Neo4jGraph;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.Orientation.REVERSE;

class NodeSimilarityStreamProcTest extends NodeSimilarityProcTest<NodeSimilarityStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<NodeSimilarity, NodeSimilarityResult, NodeSimilarityStreamConfig>> getProcedureClazz() {
        return NodeSimilarityStreamProc.class;
    }

    @Override
    public NodeSimilarityStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeSimilarityStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
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
    @MethodSource("org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityProcTest#allValidGraphVariationsWithProjections")
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
    @MethodSource("org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityProcTest#allValidGraphVariationsWithProjections")
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
    @MethodSource("org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityProcTest#allValidGraphVariationsWithProjections")
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
    @MethodSource("org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityProcTest#allValidGraphVariationsWithProjections")
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

    @Override
    @Test
    public void checkStatsModeExists() {
        applyOnProcedure((proc) -> {
            boolean inStreamClass = methodExists(proc, "stream");
            if (inStreamClass) {
                assertFalse(
                    methodExists(proc, "stats"),
                    "Stats method was moved to its own class"
                );
            }
        });
    }

    @Nested
    @TestInstance(value = TestInstance.Lifecycle.PER_METHOD)
    class NonConsecutiveIds {

        @Neo4jGraph
        private static final String DB_CYPHER_NON_CONSECUTIVE =
            "CREATE (:IncrementIdSpace)" + DB_CYPHER;

        @ParameterizedTest(name = "{1}")
        @MethodSource("org.neo4j.graphalgo.similarity.nodesim.NodeSimilarityProcTest#allGraphVariations")
        void shouldDealWithAnyIdSpace(GdsCypher.QueryBuilder queryBuilder, String testName) throws Exception {
            String graphCreate =
                "CALL gds.graph.create(" +
                "    'myGraphNATURAL'," +
                "    ['Person', 'Item']," +
                "    'LIKES'" +
                ")";

            int idOffset = 1;
            long deletedNodes = 0;
            registerProcedures(GraphDropProc.class);
            runQuery("CALL gds.graph.drop('myGraphNATURAL')");
            runQuery(graphCreate);
            GraphStore myGraphNATURAL = GraphStoreCatalog
                .get(getUsername(), db.databaseId(), "myGraphNATURAL")
                .graphStore();

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
}
