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

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher.ModeBuildStage;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.scaling.ScalarScaler;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.isA;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class PageRankWriteProcTest extends PageRankProcTest<PageRankWriteConfig> {

    @Override
    Stream<DynamicTest> modeSpecificConfigTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    public Class<? extends AlgoBaseProc<PageRankAlgorithm, PageRankResult, PageRankWriteConfig>> getProcedureClazz() {
        return PageRankWriteProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.pagerank.PageRankProcTest#graphVariations")
    void testPageRankWriteBack(ModeBuildStage queryBuilder, String testCaseName) {
        String writeProperty = "myFancyScore";
        String query = queryBuilder
            .writeMode()
            .addPlaceholder("writeProperty", "writeProp")
            .yields("writeMillis", "configuration");

        runQueryWithRowConsumer(query, MapUtil.map("writeProp", writeProperty),
            row -> {
                assertUserInput(row, "writeProperty", writeProperty);
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );

        assertWriteResult(writeProperty, expected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.pagerank.PageRankProcTest#graphVariationsWeight")
    void testWeightedPageRankWriteBack(ModeBuildStage queryBuilder, String testCaseName) {
        var writeProperty = "pagerank";
        String query = queryBuilder
            .writeMode()
            .addParameter("writeProperty", writeProperty)
            .addParameter("relationshipWeightProperty", "weight")
            .yields("writeMillis", "configuration");

        runQueryWithRowConsumer(
            query,
            row -> {
                assertUserInput(row, "writeProperty", writeProperty);
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );

        assertWriteResult(writeProperty, weightedExpected);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.pagerank.PageRankProcTest#graphVariations")
    void testWriteYields(ModeBuildStage queryBuilder, String testCaseName) {
        var writeProp = "writeProp";
        String query = queryBuilder
            .writeMode()
            .addParameter("writeProperty", writeProp)
            .addParameter("tolerance", 0.0001)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodePropertiesWritten", 10L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "writeMillis", greaterThan(-1L),
            "didConverge", false,
            "ranIterations", 20L,
            "centralityDistribution", isA(Map.class),
            "configuration", allOf(isA(Map.class), hasEntry("writeProperty", writeProp))
        )));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.gds.pagerank.PageRankProcTest#graphVariations")
    void shouldNotComputeCentralityDistributionOnLogScaler(ModeBuildStage queryBuilder, String testCaseName) {
        var writeProp = "writeProp";
        var query = queryBuilder
            .writeMode()
            .addParameter("scaler", ScalarScaler.Variant.LOG)
            .addParameter("writeProperty", writeProp)
            .yields("centralityDistribution");

        assertCypherResult(query, List.of(
            Map.of(
                "centralityDistribution", Map.of("Error", "Unable to create histogram when using scaler of type LOG")
            )
        ));
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

    private void assertWriteResult(String writeProperty, List<Map<String, Object>> expected) {
        assertCypherResult(
            formatWithLocale("MATCH (n) WHERE n.%1$s IS NOT null RETURN id(n) AS nodeId, n.%1$s AS score ORDER BY nodeId",
                writeProperty
            ),
            expected
        );
    }
}
