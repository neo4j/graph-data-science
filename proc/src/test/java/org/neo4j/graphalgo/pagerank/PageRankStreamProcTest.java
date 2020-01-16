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
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
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
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariationsEqualWeight")
    void testWeightedPageRankWithAllRelationshipsEqual(ModeBuildStage queryBuilder, String testCase) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder
            .streamMode()
            .addParameter("weightProperty", "equalWeight")
            .yields("nodeId", "score");

        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariationsLabel3")
    void testWeightedPageRankFromLoadedGraphWithDirectionBoth(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .streamMode()
            .addParameter("weightProperty", "equalWeight")
            .addParameter("maxIterations", 1)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query,
            row -> actual.put(row.getNumber("nodeId").longValue(), row.getNumber("score").doubleValue()));

        long distinctValues = actual.values().stream().distinct().count();
        assertEquals(1, distinctValues);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testWeightedPageRankThrowsIfWeightPropertyDoesNotExist(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .streamMode()
            .addParameter("weightProperty", "does_not_exist")
            .yields("nodeId", "score");

        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            runQueryWithRowConsumer(query, row -> {});
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertTrue(
            rootCause.getMessage().contains("Weight property `does_not_exist` not found in graph") ||
            rootCause.getMessage().contains("No graph was loaded for property does_not_exist"));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariationsWeight")
    void testWeightedPageRankWithCachedWeights(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .streamMode()
            .addParameter("weightProperty", "weight")
            .addParameter("cacheWeights", true)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();
        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
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
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariationsWeight")
    void testWeightedPageRank(ModeBuildStage queryBuilder, String testCaseName) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = queryBuilder
            .streamMode()
            .addParameter("weightProperty", "weight")
            .yields("nodeId", "score");

        runQueryWithRowConsumer(query,
            row -> actual.put((Long) row.get("nodeId"), (Double) row.get("score"))
        );
        assertMapEquals(weightedExpected, actual);
    }

}
