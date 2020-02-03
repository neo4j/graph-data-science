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

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

public class NodeSimilarityStatsTest extends NodeSimilarityBaseProcTest<NodeSimilarityStatsConfig>  {

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.nodesim.NodeSimilarityBaseProcTest#allGraphVariations")
    void statsShouldNotHaveWriteProperties(GdsCypher.QueryBuilder queryBuilder, String testName) {
        String query = queryBuilder
            .algo("nodeSimilarity")
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
                assertThat(config.keySet(), not(hasItems(
                    "writeProperty",
                    "writeRelationshipType"
                )));
            }
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
