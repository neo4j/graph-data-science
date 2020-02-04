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
package org.neo4j.graphalgo.nodesim;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NodeSimilarityStatsProcTest extends NodeSimilarityBaseProcTest<NodeSimilarityStatsConfig>  {

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.nodesim.NodeSimilarityBaseProcTest#allGraphVariations")
    void statsShouldNotHaveWriteProperties(GdsCypher.QueryBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .algo("nodeSimilarity")
            .statsMode()
            .yields();

        List<String> forbiddenResultColumns = Arrays.asList(
            "writeMillis",
            "nodePropertiesWritten",
            "relationshipPropertiesWritten"
        );
        List<String> forbiddenConfigKeys = Arrays.asList(
            "writeProperty",
            "writeRelationshipType"
        );
        runQueryWithResultConsumer(query, result -> {
            List<String> badResultColumns = result.columns()
                .stream()
                .filter(forbiddenResultColumns::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badResultColumns);
            assertTrue(result.hasNext(), "Result must not be empty.");
            Map<String, Object> config = (Map<String, Object>) result.next().get("configuration");
            List<String> badConfigKeys = config.keySet()
                .stream()
                .filter(forbiddenConfigKeys::contains)
                .collect(Collectors.toList());
            assertEquals(Collections.emptyList(), badConfigKeys);
        });
    }

    @Override
    public Class<? extends AlgoBaseProc<?, NodeSimilarityResult, NodeSimilarityStatsConfig>> getProcedureClazz() {
        return NodeSimilarityStatsProc.class;
    }

    @Override
    public NodeSimilarityStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return NodeSimilarityStatsConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }
}
