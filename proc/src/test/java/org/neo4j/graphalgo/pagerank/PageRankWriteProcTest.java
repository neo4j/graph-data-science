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
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.WriteConfigTests;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.pagerank.PageRank;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageRankWriteProcTest extends PageRankProcTestBase<PageRankWriteConfig> implements
    WriteConfigTests<PageRankWriteConfig, PageRank> {

    @Override
    public Class<? extends BaseAlgoProc<?, PageRank, PageRankWriteConfig>> getProcedureClazz() {
        return PageRankWriteProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphVariations")
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

    @TestSupport.AllGraphNamesTest
    void testWeightedPageRankWriteBack(String graphImpl) {
        String query = "CALL gds.algo.pageRank.write(" +
                       "    {" +
                       "        nodeProjection: 'Label1', relationshipProjection: 'TYPE1'," +
                       "        graph: $graph, weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("pagerank", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("pagerank", weightedExpected);
    }

    @TestSupport.AllGraphNamesTest
    void testPageRankWriteBackUnderDifferentProperty(String graphImpl) {
        String query = "CALL gds.algo.pageRank.write(" +
                       "    'Label1', 'TYPE1', {" +
                       "        writeProperty: 'foobar', graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("foobar", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("foobar", expected);
    }

    @TestSupport.AllGraphNamesTest
    void testPageRankParallelWriteBack(String graphImpl) {
        String query = "CALL gds.algo.pageRank.write(" +
                       "    'Label1', 'TYPE1', {" +
                       "        batchSize: 3, write: true, graph: $graph" +
                       "    }" +
                       ") YIELD writeMillis, write, writeProperty";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set")
        );
        assertResult("pagerank", expected);
    }

    @TestSupport.AllGraphNamesTest
    void testPageRankParallelExecution(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.algo.pageRank.write.stream(" +
                       "    'Label1', 'TYPE1', {" +
                       "        batchSize: 2, graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> {
                final long nodeId = row.getNumber("nodeId").longValue();
                actual.put(nodeId, (Double) row.get("score"));
            }
        );
        assertMapEquals(expected, actual);
    }

    @TestSupport.AllGraphNamesTest
    void testPageRankWithToleranceParam(String graphImpl) {
        String query;
        query = "CALL gds.algo.pageRank.write(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 0.0001, batchSize: 2, graph: $graph" +
                "     }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> assertEquals(20L, (long) row.getNumber("iterations")));

        query = "CALL gds.algo.pageRank.write(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 100.0, batchSize: 2, graph: $graph" +
                "    }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> assertEquals(1L, (long) row.getNumber("iterations")));

        query = "CALL gds.algo.pageRank.write(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 0.20010237991809848, batchSize: 2, graph: $graph" +
                "    }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> assertEquals(4L, (long) row.getNumber("iterations")));

        query = "CALL gds.algo.pageRank.write(" +
                "    'Label1', 'TYPE1', {" +
                "        tolerance: 0.20010237991809843, batchSize: 2, graph: $graph" +
                "    }" +
                ") YIELD nodes, iterations";
        runQuery(query, MapUtil.map("graph", graphImpl),
            row -> assertEquals(5L, (long) row.getNumber("iterations")));
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

//    private void assertWriteResult(Map<Long, Double> expectedScores, String writeProperty) {
//        Map<Long, Double> actualScores = new HashMap<>();
//        runQuery(String.format("MATCH (n) RETURN id(n) as id, n.%s as score", writeProperty), (row) -> {
//            double score = row.getNumber("score").doubleValue();
//            long id = row.getNumber("id").longValue();
//            actualScores.put(id, score);
//        });
//
//        assertMapEquals(expectedScores, actualScores);
//    }
}
