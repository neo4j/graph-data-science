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
package org.neo4j.gds.pagerank;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.scaling.L1Norm;
import org.neo4j.gds.scaling.L2Norm;
import org.neo4j.gds.scaling.Max;
import org.neo4j.gds.scaling.Mean;
import org.neo4j.gds.scaling.MinMax;
import org.neo4j.gds.scaling.NoneScaler;
import org.neo4j.kernel.impl.core.NodeEntity;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.number.IsCloseTo.closeTo;

class ArticleRankProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a:Label1 {name: 'a'})" +
        ", (b:Label1 {name: 'b'})" +
        ", (a)-[:TYPE1]->(b)";

    private static final String GRAPH_NAME = "graph";

    private static Stream<Arguments> estimations() {
        return Stream.of(
            Arguments.of(GdsCypher.ExecutionModes.STREAM),
            Arguments.of(GdsCypher.ExecutionModes.WRITE),
            Arguments.of(GdsCypher.ExecutionModes.MUTATE),
            Arguments.of(GdsCypher.ExecutionModes.STATS)
        );}

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(
            GraphProjectProc.class,
            ArticleRankStatsProc.class,
            ArticleRankStreamProc.class,
            ArticleRankWriteProc.class,
            ArticleRankMutateProc.class
        );
        runQuery("CALL gds.graph.project($graphName, '*', '*')", Map.of("graphName", GRAPH_NAME));
    }

    static Stream<Arguments> scalers() {
        return Stream.of(
            Arguments.of(NoneScaler.TYPE, 0.15, 0.2350),
            Arguments.of(L1Norm.TYPE, 0.38961, 0.61038),
            Arguments.of(L2Norm.TYPE, 0.53803, 0.84292),
            Arguments.of(Mean.TYPE, -0.5, 0.5),
            Arguments.of(MinMax.TYPE, 0.0, 1.0),
            Arguments.of(Max.TYPE, 0.63829, 1.0)
        );
    }

    @ParameterizedTest
    @MethodSource("scalers")
    void scalers(String scaler, double expectedNode0, double expectedNode1) {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .streamMode()
            .addParameter("scaler", scaler)
            .yields();

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "score", closeTo(expectedNode0, PageRankProcTest.RESULT_ERROR)),
            Map.of("nodeId", 1L, "score", closeTo(expectedNode1, PageRankProcTest.RESULT_ERROR))
        ));
    }

    @Test
    void invalidScaler() {
        assertThatThrownBy(() -> runQuery(GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .streamMode()
            .addParameter("scaler", "SUPERDUPERSCALARSCALERVARIANT")
            .yields())
        )
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unrecognised scaler specified: `SUPERDUPERSCALARSCALERVARIANT`.");
    }

    @Test
    void stats() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .statsMode()
            .yields();

        assertCypherResult(query, List.of(
            Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "postProcessingMillis", greaterThan(-1L),
                "configuration", isA(Map.class),
                "centralityDistribution", isA(Map.class),
                "didConverge", true,
                "ranIterations", 2L
            )));
    }

    @Test
    void stream() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .streamMode()
            .yields();

        assertCypherResult(query, List.of(
            Map.of("nodeId", 0L, "score", closeTo(0.15, PageRankProcTest.RESULT_ERROR)),
            Map.of("nodeId", 1L, "score", closeTo(0.235, PageRankProcTest.RESULT_ERROR))
        ));
    }

    @Test
    void streamWithSourceNodes() {
        var sourceNodes = allNodes();

        var query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .streamMode()
            .addPlaceholder("sourceNodes", "sources")
            .yields();

        assertCypherResult(query, Map.of("sources", sourceNodes), List.of(
            Map.of("nodeId", 0L, "score", closeTo(0.15, PageRankProcTest.RESULT_ERROR)),
            Map.of("nodeId", 1L, "score", closeTo(0.235, PageRankProcTest.RESULT_ERROR))
        ));
    }

    @Test
    void failOnMissingSourceNodes() {
        var sourceNodes = List.of(
            new NodeEntity(null, 42),
            new NodeEntity(null, 1337)
        );

        var query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .streamMode()
            .addPlaceholder("sourceNodes", "sources")
            .yields();


        assertThatThrownBy(() -> runQuery(query, Map.of("sources", sourceNodes)))
            .hasRootCauseExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Source nodes do not exist in the in-memory graph")
            .hasMessageContaining("['1337', '42']");
    }


    @Test
    void write() {
        String propertyKey = "pr";
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .writeMode()
            .addParameter("writeProperty", propertyKey)
            .yields();

        assertCypherResult(query, List.of(
            Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "writeMillis", greaterThan(-1L),
                "postProcessingMillis", greaterThan(-1L),
                "configuration", allOf(isA(Map.class), hasEntry("writeProperty", propertyKey)),
                "centralityDistribution", isA(Map.class),
                "nodePropertiesWritten", 2L,
                "didConverge", true,
                "ranIterations", 2L
            )));
    }

    @Test
    void mutate() {
        String propertyKey = "pr";
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("articleRank")
            .mutateMode()
            .addParameter("mutateProperty", propertyKey)
            .yields();

        assertCypherResult(query, List.of(
            Map.of(
                "preProcessingMillis", greaterThan(-1L),
                "computeMillis", greaterThan(-1L),
                "mutateMillis", greaterThan(-1L),
                "postProcessingMillis", greaterThan(-1L),
                "configuration", allOf(isA(Map.class), hasEntry("mutateProperty", propertyKey)),
                "centralityDistribution", isA(Map.class),
                "nodePropertiesWritten", 2L,
                "didConverge", true,
                "ranIterations", 2L
            )));
    }

    @ParameterizedTest
    @MethodSource("estimations")
    void estimates(GdsCypher.ExecutionModes mode) {
        var queryBuilder = GdsCypher.call(GRAPH_NAME)
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

        assertCypherResult(
            queryBuilder.yields("bytesMin", "bytesMax"),
            List.of(Map.of("bytesMin", greaterThan(0L), "bytesMax", greaterThan(0L)))
        );
    }
}
