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
import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.WriteConfigTests;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.pagerank.PageRank;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageRankWriteProcTest extends PageRankProcTestBase<PageRankWriteConfig> implements
    WriteConfigTests<PageRankWriteConfig, PageRank> {

    @Override
    public Class<? extends BaseAlgoProc<?, PageRank, PageRankWriteConfig>> getProcedureClazz() {
        return PageRankWriteProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariationsLabel1")
    void testPageRankWriteBack(String graphSnippet, String testCaseName) {
        String writeProperty = "myFancyScore";
        String query = "CALL gds.algo.pageRank.write(" +
                       graphSnippet +
                       "  writeProperty: $writeProp" +
                       "}) YIELD writeMillis, writeProperty";
        runQuery(query, MapUtil.map("writeProp", writeProperty),
            row -> {
                assertEquals(writeProperty, row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult(writeProperty, expected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariationsLabel1")
    void testWeightedPageRankWriteBack(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.write(" +
                       graphSnippet +
                       "        writeProperty: 'pagerank', weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD writeMillis, writeProperty";
        runQuery(
            query,
            row -> {
                assertEquals("pagerank", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("pagerank", weightedExpected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariationsLabel1")
    void testPageRankParallelWriteBack(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.write(" +
                       graphSnippet +
                       "        batchSize: 3, writeProperty: 'pagerank', graph: 'myGraph1'" +
                       "    }" +
                       ") YIELD writeMillis, writeProperty";
        runQuery(
            query,
            row -> assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set")
        );
        assertResult("pagerank", expected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariationsLabel1")
    void testPageRankWithToleranceParam(String graphSnippet, String testCaseName) {
        graphSnippet += " writeProperty: 'writeProp',";
        String graphName = "myGraph1";
        String query = "CALL gds.algo.pageRank.write(" +
                       graphSnippet +
                       "      tolerance: 0.0001, batchSize: 2, graph: $graph" +
                       "  }" +
                       ") YIELD ranIterations";
        runQuery(query, MapUtil.map("graph", graphName),
            row -> assertEquals(20L, (long) row.getNumber("ranIterations"))
        );

        query = "CALL gds.algo.pageRank.write(" +
                graphSnippet +
                "        tolerance: 100.0, batchSize: 2, graph: $graph" +
                "  }" +
                ") YIELD ranIterations";
        runQuery(query, MapUtil.map("graph", graphName),
            row -> assertEquals(1L, (long) row.getNumber("ranIterations"))
        );

        query = "CALL gds.algo.pageRank.write(" +
                graphSnippet +
                "        tolerance: 0.20010237991809848, batchSize: 2, graph: $graph" +
                "  }" +
                ") YIELD ranIterations";
        runQuery(query, MapUtil.map("graph", graphName),
            row -> assertEquals(4L, (long) row.getNumber("ranIterations"))
        );

        query = "CALL gds.algo.pageRank.write(" +
                graphSnippet +
                "        tolerance: 0.20010237991809843, batchSize: 2, graph: $graph" +
                "  }" +
                ") YIELD ranIterations";
        runQuery(query, MapUtil.map("graph", graphName),
            row -> assertEquals(5L, (long) row.getNumber("ranIterations"))
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariationsLabel1")
    void testWriteYieldRanAndMaxIterationsAndDidConverge(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.write(" +
                       graphSnippet +
                       "     writeProperty: 'writeProp', tolerance: 0.0001, batchSize: 2" +
                       "  }" +
                       ") YIELD ranIterations, didConverge, maxIterations";
        runQuery(
            query,
            row -> {
                assertEquals(20, row.getNumber("ranIterations").longValue());
                assertEquals(20, row.getNumber("maxIterations").longValue());
                assertFalse(row.getBoolean("didConverge"));
            }
        );

    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankProcTestBase#graphVariationsLabel1")
    void testStatsYieldRanAndMaxIterationsAndDidConverge(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.pageRank.stats(" +
                       graphSnippet +
                       "     writeProperty: 'writeProp', tolerance: 0.0001, batchSize: 2" +
                       "  }" +
                       ") YIELD ranIterations, didConverge, maxIterations";
        runQuery(
            query,
            row -> {
                assertEquals(20, row.getNumber("ranIterations").longValue());
                assertEquals(20, row.getNumber("maxIterations").longValue());
                assertFalse(row.getBoolean("didConverge"));
            }
        );

    }

    @Override
    public PageRankWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return PageRankWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Override
    public CypherMapWrapper createMinimallyValidConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }
}
