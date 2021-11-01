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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCutStreamConfig;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ApproxMaxKCutStreamProcTest extends ApproxMaxKCutProcTest<ApproxMaxKCutStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutStreamConfig>> getProcedureClazz() {
        return ApproxMaxKCutStreamProc.class;
    }

    @Override
    public ApproxMaxKCutStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ApproxMaxKCutStreamConfig.of(
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Test
    void testStream() {
        String streamQuery = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("gds.alpha.maxkcut")
            .streamMode()
            // Make sure we get a deterministic result.
            .addParameter("randomSeed", 1337L)
            .addParameter("concurrency", 1)
            .yields();

        Map<Long, Long> expected = Map.of(
            idFunction.of("a"), 0L,
            idFunction.of("b"), 0L,
            idFunction.of("c"), 0L,
            idFunction.of("d"), 1L,
            idFunction.of("e"), 1L,
            idFunction.of("f"), 1L,
            idFunction.of("g"), 1L
        );
        runQueryWithRowConsumer(streamQuery, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            long communityId = row.getNumber("communityId").longValue();
            assertEquals(expected.get(nodeId), communityId);
        });

    }
}
