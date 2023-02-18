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
package org.neo4j.gds.beta.k1coloring;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class K1ColoringWriteProcTest extends K1ColoringProcBaseTest<K1ColoringWriteConfig> {

    @Override
    public Class<? extends AlgoBaseProc<K1Coloring, HugeLongArray, K1ColoringWriteConfig, ?>> getProcedureClazz() {
        return K1ColoringWriteProc.class;
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        return !mapWrapper.containsKey("writeProperty")
            ? mapWrapper.withEntry("writeProperty", "color")
            : mapWrapper;
    }

    @Override
    public K1ColoringWriteConfig createConfig(CypherMapWrapper configMap) {
        return K1ColoringWriteConfig.of(configMap);
    }

    @Test
    void testWriting() {
        @Language("Cypher")
        String query = algoBuildStage()
            .writeMode()
            .addParameter("writeProperty", "color")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("preProcessingMillis").longValue());
            assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
            assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
            assertEquals(4, row.getNumber("nodeCount").longValue());
            assertEquals(2, row.getNumber("colorCount").longValue());
            assertUserInput(row, "writeProperty", "color");
            assertTrue(row.getBoolean("didConverge"));
            assertTrue(row.getNumber("ranIterations").longValue() < 3);
        });

        Map<Long, Long> coloringResult = new HashMap<>(4);
        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.color AS color", row -> {
            long nodeId = row.getNumber("id").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(nodeId, color);
        });

        assertNotEquals(coloringResult.get(0L), coloringResult.get(1L));
        assertNotEquals(coloringResult.get(0L), coloringResult.get(2L));
    }

    @Test
    void testWritingEstimate() {
        @Language("Cypher")
        String query = algoBuildStage()
            .estimationMode(GdsCypher.ExecutionModes.WRITE)
            .addParameter("writeProperty", "color")
            .yields("requiredMemory", "treeView", "bytesMin", "bytesMax");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);

            String bytesHuman = MemoryUsage.humanReadable(row.getNumber("bytesMin").longValue());
            assertNotNull(bytesHuman);
            assertTrue(row.getString("requiredMemory").contains(bytesHuman));
        });
    }

    static Stream<Arguments> communitySizeInputs() {
        return Stream.of(
                Arguments.of(Map.of("minCommunitySize", 1), Map.of(
                        0L, 1L,
                        1L, 0L,
                        2L, 0L,
                        3L, 0L
                )),
                Arguments.of(Map.of("minCommunitySize", 2), Map.of(
                        1L, 0L,
                        2L, 0L,
                        3L, 0L
                ))
        );
    }

    @ParameterizedTest
    @MethodSource("communitySizeInputs")
    void testWriteWithMinCommunitySize(Map<String, Long> parameter, Map<Long, Long> expectedResult) {
        @Language("Cypher")
        String query = algoBuildStage()
                .writeMode()
                .addParameter("writeProperty", "color")
                .addAllParameters(parameter)
                .yields("nodeCount", "colorCount");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(4L, row.getNumber("nodeCount"));
            assertEquals(2L, row.getNumber("colorCount"));
        });

        runQueryWithRowConsumer("MATCH (n) RETURN id(n) AS id, n.color AS color", row -> {
            long nodeId = row.getNumber("id").longValue();
            assertEquals(expectedResult.get(nodeId), row.getNumber("color"));
        });
    }
}
