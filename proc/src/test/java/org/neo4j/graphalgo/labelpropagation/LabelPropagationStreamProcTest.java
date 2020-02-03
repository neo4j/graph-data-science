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
package org.neo4j.graphalgo.labelpropagation;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class LabelPropagationStreamProcTest extends LabelPropagationBaseProcTest<LabelPropagationStreamConfig> {
    @Override
    public Class<? extends AlgoBaseProc<?, LabelPropagation, LabelPropagationStreamConfig>> getProcedureClazz() {
        return LabelPropagationStreamProc.class;
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.labelpropagation.LabelPropagationBaseProcTest#gdsGraphVariations")
    void testStream(
        GdsCypher.QueryBuilder queryBuilder,
        String desc
    ) {

        String query = queryBuilder
            .algo("gds.labelPropagation")
            .streamMode()
            .yields();

        List<Long> actualCommunities = new ArrayList<>();
        runQueryWithRowConsumer(query, row -> {
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
            .addAllParameters(createMinimalConfig(CypherMapWrapper.create(MapUtil.map("concurrency", 4))).toMap())
            .yields(Arrays.asList("bytesMin", "bytesMax", "nodeCount", "relationshipCount"));

        assertCypherResult(query, Arrays.asList(MapUtil.map(
            "nodeCount", 12L,
            "relationshipCount", 10L,
            "bytesMin", 1720L,
            "bytesMax", 2232L
        )));
    }

    @Test
    void statsShouldNotHaveWriteProperties() {
        String query = GdsCypher
            .call()
            .explicitCreation(TEST_GRAPH_NAME)
            .algo("labelPropagation")
            .statsMode()
            .yields();

        runQueryWithResultConsumer(query, result -> {
            assertThat(result.columns(), not(hasItems(
                "writeMillis",
                "nodePropertiesWritten",
                "relationshipPropertiesWritten"
            )));

            if(result.hasNext()) {
                Map<String, Object> config = (Map<String, Object>) result.next().get("configuration");
                assertFalse(config.containsKey("writeProperty"));
            }
        });
    }

    @Override
    public LabelPropagationStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LabelPropagationStreamConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }
}
