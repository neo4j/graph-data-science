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
package org.neo4j.graphalgo.beta.k1coloring;

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class K1ColoringStreamProcTest extends K1ColoringProcBaseTest {

    @Override
    void registerProcs() throws Exception {
        registerProcedures(K1ColoringStreamProc.class);
    }

    @Test
    void testStreamingImplicit() {
        @Language("Cypher")
        String yields = algoBuildStage()
            .streamMode()
            .yields("nodeId", "color");

        Map<Long, Long> coloringResult = new HashMap<>(4);
        runQueryWithRowConsumer(yields, (row) -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long color = row.getNumber("color").longValue();
            coloringResult.put(nodeId, color);
        });

        assertNotEquals(coloringResult.get(0L), coloringResult.get(1L));
        assertNotEquals(coloringResult.get(0L), coloringResult.get(2L));
    }

    @Test
    void testStreamingEstimate() {
        @Language("Cypher")
        String query = algoBuildStage()
            .estimationMode(GdsCypher.ExecutionModes.STREAM)
            .yields("requiredMemory", "treeView", "bytesMin", "bytesMax");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("bytesMin").longValue() > 0);
            assertTrue(row.getNumber("bytesMax").longValue() > 0);

            String bytesHuman = MemoryUsage.humanReadable(row.getNumber("bytesMin").longValue());
            assertNotNull(bytesHuman);
            assertTrue(row.getString("requiredMemory").contains(bytesHuman));
        });
    }
}
