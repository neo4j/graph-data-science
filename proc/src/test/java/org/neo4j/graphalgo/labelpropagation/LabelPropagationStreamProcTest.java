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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseAlgoProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LabelPropagationStreamProcTest extends LabelPropagationProcTestBase<LabelPropagationStreamConfig> {
    @Override
    public Class<? extends BaseAlgoProc<?, LabelPropagation, LabelPropagationStreamConfig>> getProcedureClazz() {
        return LabelPropagationStreamProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("graphVariations")
    void testStream(String graphSnippet, String testCaseName) {
        String query = "CALL gds.algo.labelPropagation.stream(" +
                       graphSnippet +
                       "    maxIterations: 10" +
                       "}) YIELD nodeId, communityId";

        List<Long> actualCommunities = new ArrayList<>();
        runQuery(query, row -> {
            int id = row.getNumber("nodeId").intValue();
            long community = row.getNumber("communityId").longValue();
            actualCommunities.add(id, community);
        });

        assertEquals(actualCommunities, RESULT);
    }

    @Test
    void testEstimate() {
        String query = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .streamEstimation()
            .addAllParameters(createMinimallyValidConfig(CypherMapWrapper.empty()).toMap())
            .yields(Arrays.asList("bytesMin", "bytesMax", "nodeCount", "relationshipCount"));

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1720L,
            "bytesMax", 2232L
        )));
    }

    @Override
    public LabelPropagationStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }
}
