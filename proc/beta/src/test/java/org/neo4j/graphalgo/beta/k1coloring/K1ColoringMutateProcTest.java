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
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GraphMutationTest;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class K1ColoringMutateProcTest extends K1ColoringProcBaseTest implements GraphMutationTest<K1ColoringMutateConfig, HugeLongArray> {

    @Override
    public String mutateProperty() {
        return "color";
    }

    @Override
    void registerProcs() throws Exception {
        registerProcedures(K1ColoringMutateProc.class);
    }

    @Override
    public String expectedMutatedGraph() {
        return
            "  (x { color: 0 }) " +
            ", (y { color: 0 }) " +
            ", (z { color: 0 }) " +
            ", (w { color: 1 })-->(y) " +
            ", (w { color: 1 })-->(z) ";
    }

    @Override
    public K1ColoringMutateConfig createConfig(CypherMapWrapper mapWrapper) {
        return K1ColoringMutateConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public Class<? extends AlgoBaseProc<?, HugeLongArray, K1ColoringMutateConfig>> getProcedureClazz() {
        return K1ColoringMutateProc.class;
    }

    @Override
    public GraphDatabaseAPI graphDb() {
        return db;
    }

    @Override
    public void assertResultEquals(HugeLongArray result1, HugeLongArray result2) {
        assertArrayEquals(result1.toArray(), result2.toArray());
    }

    @Test
    void testMutateYields() {
        @Language("Cypher")
        String query = algoBuildStage()
            .mutateMode()
            .addParameter("mutateProperty", mutateProperty())
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertEquals(4, row.getNumber("nodeCount").longValue(), "wrong nodeCount");
            assertTrue(row.getBoolean("didConverge"), "did not converge");
            assertTrue(row.getNumber("ranIterations").longValue() < 3, "wrong ranIterations");
            assertEquals(2, row.getNumber("colorCount").longValue(), "wrong color count");
            assertTrue(row.getNumber("createMillis").longValue() >= 0, "invalid createMillis");
            assertTrue(row.getNumber("mutateMillis").longValue() >= 0, "invalid mutateMillis");
            assertTrue(row.getNumber("computeMillis").longValue() >= 0, "invalid computeMillis");
        });
    }

    @Test
    void testMutateEstimate() {
        @Language("Cypher")
        String query = algoBuildStage()
            .mutateEstimation()
            .addParameter("mutateProperty", "color")
            .yields("nodeCount", "bytesMin", "bytesMax", "requiredMemory");

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 4L,
            "bytesMin", 304056L,
            "bytesMax", 304056L,
            "requiredMemory", "296 KiB"
        )));
    }
}
