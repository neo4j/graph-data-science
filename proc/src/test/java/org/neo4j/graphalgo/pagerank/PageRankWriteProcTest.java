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
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.GdsCypher.ModeBuildStage;
import org.neo4j.graphalgo.WriteConfigTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.pagerank.PageRank;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PageRankWriteProcTest extends PageRankBaseProcTest<PageRankWriteConfig> implements
    WriteConfigTest<PageRankWriteConfig, PageRank> {

    @Override
    public Class<? extends AlgoBaseProc<?, PageRank, PageRankWriteConfig>> getProcedureClazz() {
        return PageRankWriteProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testPageRankWriteBack(ModeBuildStage queryBuilder, String testCaseName) {
        String writeProperty = "myFancyScore";
        String query = queryBuilder
            .writeMode()
            .addPlaceholder("writeProperty", "writeProp")
            .yields("writeMillis", "writeProperty");

        runQueryWithRowConsumer(query, MapUtil.map("writeProp", writeProperty),
            row -> {
                assertEquals(writeProperty, row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult(writeProperty, expected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariationsWeight")
    void testWeightedPageRankWriteBack(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .writeMode()
            .addParameter("writeProperty", "pagerank")
            .addParameter("weightProperty", "weight")
            .yields("writeMillis", "writeProperty");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals("pagerank", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("pagerank", weightedExpected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testPageRankParallelWriteBack(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .writeMode()
            .addParameter("writeProperty", "pagerank")
            .yields("writeMillis", "writeProperty");

        runQueryWithRowConsumer(
            query,
            row -> assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set")
        );
        assertResult("pagerank", expected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testPageRankWithToleranceParam(ModeBuildStage queryBuilder, String testCaseName) {
        GdsCypher.ParametersBuildStage builder = queryBuilder
            .writeMode()
            .addParameter("writeProperty", "writeProp");
        String query = builder
            .addParameter("tolerance", 0.0001)
            .yields("ranIterations");

        runQueryWithRowConsumer(query,
            row -> assertEquals(20L, (long) row.getNumber("ranIterations"))
        );

        query = builder
            .addParameter("tolerance", 100.0)
            .yields("ranIterations");

        runQueryWithRowConsumer(query,
            row -> assertEquals(1L, (long) row.getNumber("ranIterations"))
        );

        query = builder
            .addParameter("tolerance", 0.20010237991809848)
            .yields("ranIterations");

        runQueryWithRowConsumer(query,
            row -> assertEquals(4L, (long) row.getNumber("ranIterations"))
        );

        query = builder
            .addParameter("tolerance", 0.20010237991809843)
            .yields("ranIterations");

        runQueryWithRowConsumer(query,
            row -> assertEquals(5L, (long) row.getNumber("ranIterations"))
        );
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testWriteYieldRanAndMaxIterationsAndDidConverge(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .writeMode()
            .addParameter("writeProperty", "writeProp")
            .addParameter("tolerance", 0.0001)
            .yields("ranIterations", "didConverge", "maxIterations");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(20, row.getNumber("ranIterations").longValue());
                assertEquals(20, row.getNumber("maxIterations").longValue());
                assertFalse(row.getBoolean("didConverge"));
            }
        );

    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.pagerank.PageRankBaseProcTest#graphVariations")
    void testStatsYieldRanAndMaxIterationsAndDidConverge(ModeBuildStage queryBuilder, String testCaseName) {
        String query = queryBuilder
            .writeMode()
            .addParameter("writeProperty", "writeProp")
            .addParameter("tolerance", 0.0001)
            .yields("ranIterations", "didConverge", "maxIterations", "dampingFactor");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertEquals(20, row.getNumber("ranIterations").longValue());
                assertEquals(20, row.getNumber("maxIterations").longValue());
                assertEquals(0.85D, row.getNumber("dampingFactor").doubleValue());
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
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }
}
