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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher.ModeBuildStage;
import org.neo4j.graphalgo.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageRankStreamProcTest extends PageRankProcTest<PageRankStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<?, PageRank, PageRankStreamConfig>> getProcedureClazz() {
        return PageRankStreamProc.class;
    }

    @Override
    public PageRankStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariations")
    void testPageRankParallelExecution(ModeBuildStage queryBuilder, String testName) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder.streamMode().yields("nodeId", "score");

        runQueryWithRowConsumer(query,
            row -> {
                final long nodeId = row.getNumber("nodeId").longValue();
                actual.put(nodeId, (Double) row.get("score"));
            }
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariationsEqualWeight")
    void testWeightedPageRankWithAllRelationshipsEqual(ModeBuildStage queryBuilder, String testCase) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder
            .streamMode()
            .addParameter("relationshipWeightProperty", "equalWeight")
            .yields("nodeId", "score");

        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariationsLabel3")
    void testWeightedPageRankFromLoadedGraphWithDirectionBoth(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .streamMode()
            .addParameter("relationshipWeightProperty", "equalWeight")
            .addParameter("maxIterations", 1)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query,
            row -> actual.put(row.getNumber("nodeId").longValue(), row.getNumber("score").doubleValue()));

        long distinctValues = actual.values().stream().distinct().count();
        assertEquals(1, distinctValues);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariations")
    void testWeightedPageRankThrowsIfWeightPropertyDoesNotExist(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .streamMode()
            .addParameter("relationshipWeightProperty", "does_not_exist")
            .yields("nodeId", "score");

        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            runQueryWithRowConsumer(query, row -> {});
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertTrue(
            rootCause.getMessage().contains("Relationship weight property `does_not_exist` not found in graph") ||
            rootCause.getMessage().contains("No graph was loaded for property does_not_exist"));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariationsWeight")
    void testWeightedPageRankWithCachedWeights(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("cacheWeights", true)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariations")
    void testPageRank(ModeBuildStage queryBuilder, String testCaseName) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder
            .streamMode()
            .addParameter("dampingFactor", 0.85)
            .yields("nodeId", "score");

        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariationsWeight")
    void testWeightedPageRank(ModeBuildStage queryBuilder, String testCaseName) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder
            .streamMode()
            .addParameter("relationshipWeightProperty", "weight")
            .yields("nodeId", "score");

        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariations")
    void testStatsYieldRanAndMaxIterationsAndDidConverge(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .statsMode()
            .addParameter("tolerance", 0.0001)
            .yields("ranIterations", "didConverge", "configuration");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(20, row.getNumber("ranIterations").longValue());
                assertUserInput(row, "maxIterations", 20);
                assertUserInput(row, "dampingFactor", 0.85D);
                assertFalse(row.getBoolean("didConverge"));
            }
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTest#graphVariations")
    void statsShouldNotHaveWriteProperties(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .statsMode()
            .yields();

        List<String> forbiddenResultColumns = Arrays.asList(
            "writeMillis",
            "nodePropertiesWritten",
            "relationshipPropertiesWritten"
        );
        List<String> forbiddenConfigKeys = Collections.singletonList("writeProperty");
        runQueryWithResultConsumer(query, result -> {
            List<String> badResultColumns = result.columns()
                .stream()
                .filter(forbiddenResultColumns::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badResultColumns);
            assertTrue(result.hasNext(), "Result must not be empty.");
            Map<String, Object> config = (Map<String, Object>) result.next().get("configuration");
            List<String> badConfigKeys = config.keySet()
                .stream()
                .filter(forbiddenConfigKeys::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badConfigKeys);
        });
    }
}
