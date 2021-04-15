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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

public class ArticleRankProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Label1 {name: 'a'})" +
        ", (b:Label1 {name: 'b'})" +
        ", (a)-[:TYPE1]->(b)";
    public static final String GRAPH_NAME = "graph";

    private static Stream<Arguments> estimations() {
        Map<String, Long> expected = Map.of("bytesMin", 544L, "bytesMax", 544L);
        return Stream.of(
        Arguments.of(GdsCypher.ExecutionModes.STREAM, expected),
        Arguments.of(GdsCypher.ExecutionModes.WRITE, expected),
        Arguments.of(GdsCypher.ExecutionModes.MUTATE, expected)
    );}

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(GraphCreateProc.class, ArticleRankStreamProc.class, ArticleRankWriteProc.class, ArticleRankMutateProc.class);
        runQuery("CALL gds.graph.create($graphName, '*', '*')", Map.of("graphName", GRAPH_NAME));
    }

    @Test
    void stream() {
        String query = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("articleRank")
            .streamMode()
            .yields();

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "score", 0.15000000000000002),
            Map.of("nodeId", 1L, "score", 0.19250000000000003)
        ));
    }

    @Test
    void write() {
        String propertyKey = "pr";
        String query = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("articleRank")
            .writeMode()
            .addParameter("writeProperty", propertyKey)
            .yields();

        assertCypherResult(query, List.of(
            Map.of(
                "createMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "writeMillis", greaterThan(-1L),
                "postProcessingMillis", greaterThan(-1L),
                "configuration", isA(Map.class),
                "centralityDistribution", isA(Map.class),
                "nodePropertiesWritten", 2L,
                "didConverge", true,
                "ranIterations", 2L
            )));
    }

    @Test
    void mutate() {
        String propertyKey = "pr";
        String query = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("articleRank")
            .mutateMode()
            .addParameter("mutateProperty", propertyKey)
            .yields();

        assertCypherResult(query, List.of(
            Map.of(
                "createMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "mutateMillis", greaterThan(-1L),
                "postProcessingMillis", greaterThan(-1L),
                "configuration", isA(Map.class),
                "centralityDistribution", isA(Map.class),
                "nodePropertiesWritten", 2L,
                "didConverge", true,
                "ranIterations", 2L
            )));
    }

    @ParameterizedTest
    @MethodSource("estimations")
    void estimates(GdsCypher.ExecutionModes mode, Map<String, Object> expected) {
        var queryBuilder = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("articleRank")
            .estimationMode(mode);

        switch (mode) {
            case WRITE:
                queryBuilder = queryBuilder.addParameter("writeProperty", "pr");
                break;
            case MUTATE:
                queryBuilder = queryBuilder.addParameter("mutateProperty", "pr");
                break;
            default:
                break;
        }

        assertCypherResult(queryBuilder.yields("bytesMin", "bytesMax"), List.of(expected));
    }
}
