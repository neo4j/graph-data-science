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
package org.neo4j.gds.approxmaxkcut;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.ThrowableRootCauseMatcher.rootCause;

class ApproxMaxKCutStreamProcTest extends ApproxMaxKCutProcTest<ApproxMaxKCutStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutStreamConfig, ?>> getProcedureClazz() {
        return ApproxMaxKCutStreamProc.class;
    }

    @Override
    public ApproxMaxKCutStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ApproxMaxKCutStreamConfig.of(mapWrapper);
    }

    @Test
    void testStream() {
        String streamQuery = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.maxkcut")
            .streamMode()
            // Make sure we get a deterministic result.
            .addParameter("randomSeed", 1337L)
            .addParameter("concurrency", 1)
            .yields();

        Map<Long, Long> expected = Map.of(
            idFunction.of("a"), 1L,
            idFunction.of("b"), 1L,
            idFunction.of("c"), 1L,
            idFunction.of("d"), 0L,
            idFunction.of("e"), 0L,
            idFunction.of("f"), 0L,
            idFunction.of("g"), 0L
        );

        runQueryWithRowConsumer(streamQuery, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long communityId = row.getNumber("communityId").longValue();
            assertEquals(expected.get(nodeId), communityId);
        });
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), Map.of(
                        0L, 1L,
                        1L, 1L,
                        2L, 1L,
                        3L, 0L,
                        4L, 0L,
                        5L, 0L,
                        6L, 0L
                )),
                Arguments.of(Map.of("minCommunitySize", 4), Map.of(
                        3L, 0L,
                        4L, 0L,
                        5L, 0L,
                        6L, 0L
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testStreamWithMinCommunitySize(Map<String, Long> parameter, Map<Long, Long> expectedResult) {
        String streamQuery = GdsCypher.call(GRAPH_NAME)
                .algo("gds.alpha.maxkcut")
                .streamMode()
                // Make sure we get a deterministic result.
                .addParameter("randomSeed", 1337L)
                .addParameter("concurrency", 1)
                .addAllParameters(parameter)
                .yields();

        runQueryWithRowConsumer(streamQuery, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long communityId = row.getNumber("communityId").longValue();
            assertEquals(expectedResult.get(nodeId), communityId);
        });
    }

    // Min k-cut capabilities not exposed in API yet.
    @Disabled
    @Test
    void testIllegalMinCommunitySizesSum() {
        String streamQuery = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.maxkcut")
            .streamMode()
            .addParameter("minCommunitySizes", List.of(100, 100))
            .yields();

        QueryExecutionException exception = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(streamQuery)
        );

        assertThat(
            exception,
            rootCause(
                IllegalArgumentException.class,
                "The sum of min community sizes is larger than half of the number of nodes in the graph: 200 > 3"
            )
        );
    }
}
