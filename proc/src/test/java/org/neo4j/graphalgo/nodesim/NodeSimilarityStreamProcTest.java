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
package org.neo4j.graphalgo.nodesim;

import org.apache.commons.compress.utils.Sets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphalgo.newapi.GraphDropProc;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.Projection.REVERSE;

class NodeSimilarityStreamProcTest extends NodeSimilarityBaseProcTest<NodeSimilarityStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<?, NodeSimilarityResult, NodeSimilarityStreamConfig>> getProcedureClazz() {
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

    private static String resultString(long node1, long node2, double similarity) {
        return String.format("%d,%d %f%n", node1, node2, similarity);
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

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.nodesim.NodeSimilarityBaseProcTest#allGraphVariations")
    void shouldDealWithAnyIdSpace(GdsCypher.QueryBuilder queryBuilder, String testName) throws Exception {
        String graphCreate =
            "CALL gds.graph.create(" +
            "    'myGraphNATURAL'," +
            "    'Person | Item'," +
            "    'LIKES'" +
            ")";

        int idOffset = 100;
        GraphDatabaseAPI localDb = TestDatabaseCreator.createTestDatabase();
        registerProcedures(localDb, NodeSimilarityStreamProc.class, GraphCreateProc.class, GraphDropProc.class);
        QueryRunner.runQuery(localDb, "MATCH (n) DETACH DELETE n");
        QueryRunner.runQuery(localDb, String.format("UNWIND range(1, %d) AS i CREATE (:IncrementIdSpace)", idOffset));
        QueryRunner.runQuery(localDb, DB_CYPHER);
        QueryRunner.runQuery(localDb, "CALL gds.graph.drop('myGraphNATURAL')");
        QueryRunner.runQuery(localDb, graphCreate);
        QueryRunner.runQuery(localDb, "MATCH (n:IncrementIdSpace) DELETE n");

        HashSet<String> expected = Sets.newHashSet(
            resultString(idOffset + 0, idOffset + 1, 2 / 3.0),
            resultString(idOffset + 0, idOffset + 2, 1 / 3.0),
            resultString(idOffset + 1, idOffset + 2, 0.0),
            resultString(idOffset + 1, idOffset + 0, 2 / 3.0),
            resultString(idOffset + 2, idOffset + 0, 1 / 3.0),
            resultString(idOffset + 2, idOffset + 1, 0.0)
        );

        String query = queryBuilder
            .algo("nodeSimilarity")
            .streamMode()
            .addParameter("similarityCutoff", 0.0)
            .yields("node1", "node2", "similarity");

        Collection<String> result = new HashSet<>();
        runQueryWithRowConsumer(localDb, query, row -> {
                long node1 = row.getNumber("node1").longValue();
                long node2 = row.getNumber("node2").longValue();
                double similarity = row.getNumber("similarity").doubleValue();
                result.add(resultString(node1, node2, similarity));
            }
        );

        assertEquals(expected, result);
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("org.neo4j.graphalgo.nodesim.NodeSimilarityBaseProcTest#allValidGraphVariationsWithProjections")
    void shouldStreamResults(GdsCypher.QueryBuilder queryBuilder, Projection projection, String testName) {
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
            projection == REVERSE
                ? EXPECTED_INCOMING
                : EXPECTED_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("org.neo4j.graphalgo.nodesim.NodeSimilarityBaseProcTest#allValidGraphVariationsWithProjections")
    void shouldStreamTopResults(GdsCypher.QueryBuilder queryBuilder, Projection projection, String testName) {
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
            projection == REVERSE
                ? EXPECTED_TOP_INCOMING
                : EXPECTED_TOP_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{2}")
    @MethodSource("org.neo4j.graphalgo.nodesim.NodeSimilarityBaseProcTest#allValidGraphVariationsWithProjections")
    void shouldIgnoreParallelEdges(GdsCypher.QueryBuilder queryBuilder, Projection projection, String testName) {
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
            projection == REVERSE
                ? EXPECTED_INCOMING
                : EXPECTED_OUTGOING,
            result
        );
    }
}
