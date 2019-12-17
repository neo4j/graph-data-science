/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.impl.pagerank.PageRank;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageRankStreamProcTest extends PageRankBaseProcTest<PageRankStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<?, PageRank, PageRankStreamConfig>> getProcedureClazz() {
        return PageRankStreamProc.class;
    }

    @Override
    public PageRankStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testPageRankParallelExecution(String graphSnippet, String testName) {
        final Map<Long, Double> actual = new HashMap<>();

        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        batchSize: 2, graph: 'graphLabel1'" +
                       "    }" +
                       ") YIELD nodeId, score";

        runQuery(query,
            row -> {
                final long nodeId = row.getNumber("nodeId").longValue();
                actual.put(nodeId, (Double) row.get("score"));
            }
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testWeightedPageRankWithAllRelationshipsEqual(String graphSnippet, String testCase) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        weightProperty: 'equalWeight'" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariationsLabel3")
    void testWeightedPageRankFromLoadedGraphWithDirectionBoth(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        weightProperty: 'equalWeight', " +
                       "        iterations: 1" +

                        "    }" +
                       ") YIELD nodeId, score";

        final Map<Long, Double> actual = new HashMap<>();
        runQuery(query,
            row -> actual.put(row.getNumber("nodeId").longValue(), row.getNumber("score").doubleValue()));

        long distinctValues = actual.values().stream().distinct().count();
        assertEquals(1, distinctValues);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariations")
    void testWeightedPageRankThrowsIfWeightPropertyDoesNotExist(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        weightProperty: 'does_not_exist' " +
                       "    }" +
                       ") YIELD nodeId, score";

        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            runQuery(query, row -> {});
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertTrue(
            rootCause.getMessage().contains("Weight property `does_not_exist` not found in graph") ||
            rootCause.getMessage().contains("No graph was loaded for property does_not_exist"));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testWeightedPageRankWithCachedWeights(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        weightProperty: 'weight', cacheWeights: true " +
                       "    }" +
                       ") YIELD nodeId, score";

        final Map<Long, Double> actual = new HashMap<>();
        runQuery(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testPageRank(String graphSnippet, String testCaseName) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        dampingFactor: 0.85" +
                       "    }" +
                       ") YIELD nodeId, score";

        runQuery(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testWeightedPageRank(String graphSnippet, String testCaseName) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.algo.pageRank.stream(" +
                       graphSnippet +
                       "        weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD nodeId, score";

        runQuery(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }


}
