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
package org.neo4j.graphalgo.labelpropagation;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.WriteConfigTests;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LabelPropagationWriteProcTest extends LabelPropagationProcTestBase<LabelPropagationWriteConfig> implements
    WriteConfigTests<LabelPropagationWriteConfig, LabelPropagation> {

    @Override
    public Class<? extends BaseAlgoProc<?, LabelPropagation, LabelPropagationWriteConfig>> getProcedureClazz() {
        return LabelPropagationWriteProc.class;
    }

    @Override
    public LabelPropagationWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationWriteConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Override
    public CypherMapWrapper createMinimallyValidConfig(CypherMapWrapper mapWrapper) {
        // TODO: generalise for all WriteProcTests
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", "writeProperty");
        }
        return mapWrapper;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.labelpropagation.LabelPropagationProcTestBase#graphVariations")
    void respectsMaxIterations(String graphSnippet, String desc) {
        String query = "CALL gds.algo.labelPropagation.write(" +
                       graphSnippet +
                       "    maxIterations: 5, " +
                       "    writeProperty: 'label'" +
                       "  }" +
                       ")";
        runQuery(
            query,
            row -> {
                assertTrue(5 >= row.getNumber("ranIterations").intValue());
                assertEquals(
                    MapUtil.map(
                        "p999", 2L,
                        "p99", 2L,
                        "p95", 2L,
                        "p90", 2L,
                        "p75", 1L,
                        "p50", 1L,
                        "min", 1L,
                        "max", 2L,
                        "mean", 1.2D
                    ),
                    row.get("communityDistribution")
                );
            }
        );

    }

    static Stream<Arguments> concurrenciesExplicitAndImplicitCreate() {
        return TestSupport.crossArguments(() -> Stream.of(1, 4, 8).map(Arguments::of),
            LabelPropagationProcTestBase::graphVariations
        );
    }

    @ParameterizedTest(name = "concurrency = {0}, {2}")
    @MethodSource("concurrenciesExplicitAndImplicitCreate")
    void shouldRunLabelPropagationNatural(int concurrency, String graphSnippet, String desc) {
        @Language("Cypher")
        String query = "CALL gds.algo.labelPropagation.write(" +
                       graphSnippet +
                       "        concurrency: $concurrency, seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: $writeProperty" +
                       "    }" +
                       ")";

        runQuery(query, concurrencySeedWeightAndWriteParams(concurrency),
            row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 8L,
                        "p99", 8L,
                        "p95", 8L,
                        "p90", 8L,
                        "p75", 8L,
                        "p50", 4L,
                        "min", 4L,
                        "max", 8L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.community AS community";
        runQuery(check, row -> {
            assertEquals(2, row.getNumber("community").intValue());
        });
    }

    @Test
    void shouldRunLabelPropagationUndirected() {
        String graphName = "myGraphUndirected";
        String writeProperty = "community";

        runQuery(createGraphQuery(Projection.UNDIRECTED, graphName));
        @Language("Cypher")
        String query = "CALL gds.algo.labelPropagation.write(" +
                       "        $graph, {"+
                       "         writeProperty: $writeProperty" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("graph", graphName, "writeProperty", writeProperty),
            row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 6L,
                        "p99", 6L,
                        "p95", 6L,
                        "p90", 6L,
                        "p75", 6L,
                        "p50", 6L,
                        "min", 6L,
                        "max", 6L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String check = String.format("MATCH (a {id: 0}), (b {id: 1}) " +
                       "RETURN a.%1$s AS a, b.%1$s AS b", writeProperty);
        runQuery(check, row -> {
            assertEquals(2, row.getNumber("a").intValue());
            assertEquals(7, row.getNumber("b").intValue());
        });
    }

    @Test
    void shouldRunLabelPropagationReverse() {
        String graphName = "myGraphUndirected";
        String writeProperty = "community";

        runQuery(createGraphQuery(Projection.REVERSE, graphName));
        @Language("Cypher")
        String query = "CALL gds.algo.labelPropagation.write(" +
                       "        $graph, {" +
                       "         writeProperty: $writeProperty, direction: 'INCOMING'" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("graph", graphName, "writeProperty", writeProperty),
            row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 6L,
                        "p99", 6L,
                        "p95", 6L,
                        "p90", 6L,
                        "p75", 6L,
                        "p50", 6L,
                        "min", 6L,
                        "max", 6L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String validateQuery = String.format(
            "MATCH (n) RETURN n.%1$s AS community, count(*) AS communitySize",
            writeProperty
        );
        assertCypherResult(validateQuery, Arrays.asList(
            MapUtil.map("community", 0L, "communitySize", 6L),
            MapUtil.map("community", 1L, "communitySize", 6L)
        ));
    }

    @ParameterizedTest(name = "concurrency = {0}, {2}")
    @MethodSource("concurrenciesExplicitAndImplicitCreate")
    void shouldRunLabelPropagationWithIdenticalSeedAndWriteProperties(int concurrency, String graphSnippet, String desc) {
        String query = "CALL gds.algo.labelPropagation.write(" +
                       graphSnippet +
                       "        concurrency: $concurrency, seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: $seedProperty" +
                       "    }" +
                       ")";

        runQuery(query, concurrencySeedWeightAndWriteParams(concurrency),
            row -> {
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                assertEquals("seed", row.getString("seedProperty"));
                assertEquals("seed", row.getString("writeProperty"));
                checkMillisSet(row);
                assertEquals(
                    MapUtil.map(
                        "p999", 8L,
                        "p99", 8L,
                        "p95", 8L,
                        "p90", 8L,
                        "p75", 8L,
                        "p50", 4L,
                        "min", 4L,
                        "max", 8L,
                        "mean", 6.0D
                    ),
                    row.get("communityDistribution")
                );

            }
        );
        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.seed AS community";
        runQuery(check, row -> assertEquals(2, row.getNumber("community").intValue()));
    }

    @ParameterizedTest(name = "concurrency = {0}, {2}")
    @MethodSource("concurrenciesExplicitAndImplicitCreate")
    void shouldRunLabelPropagationWithoutInitialSeed(int concurrency, String graphSnippet, String desc) {
        String query = "CALL gds.algo.labelPropagation.write(" +
                        graphSnippet +
                       "        concurrency: $concurrency, weightProperty: $weightProperty, writeProperty: $writeProperty" +
                       "    }" +
                       ")";

        runQuery(query, concurrencySeedWeightAndWriteParams(concurrency),
            row -> {
                assertNull(row.getString("seedProperty"));
                assertEquals(12, row.getNumber("nodePropertiesWritten").intValue());
                checkMillisSet(row);
            }
        );
        runQuery(
            "MATCH (n) WHERE n.id = 0 RETURN n.community AS community",
            row -> assertEquals(6, row.getNumber("community").intValue())
        );
        runQuery(
            "MATCH (n) WHERE n.id = 1 RETURN n.community AS community",
            row -> assertEquals(11, row.getNumber("community").intValue())
        );
    }

    Map<String, Object> concurrencySeedWeightAndWriteParams(int concurrency) {
        return MapUtil.map(
            "concurrency", concurrency,
            "seedProperty", "seed",
            "weightProperty", "weight",
            "writeProperty", "community"
        );
    }
}
