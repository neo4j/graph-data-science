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
package org.neo4j.graphalgo.louvain;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.louvain.Louvain;
import org.neo4j.gds.louvain.LouvainStatsConfig;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.assertj.ConditionFactory.containsAllEntriesOf;
import static org.neo4j.graphalgo.assertj.ConditionFactory.containsExactlyInAnyOrderEntriesOf;

class LouvainStatsProcTest extends LouvainProcTest<LouvainStatsConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Louvain, Louvain, LouvainStatsConfig>> getProcedureClazz() {
        return LouvainStatsProc.class;
    }

    @Override
    public LouvainStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return LouvainStatsConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void yields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("louvain")
            .statsMode()
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "ranLevels", 1L,
            "modularity", closeTo(0.3744, 1e-5),
            "modularities", contains((closeTo(0.3744, 1e-5))),
            "communityCount", 4L,
            "communityDistribution", containsExactlyInAnyOrderEntriesOf(Map.of(
                "min", 2L,
                "max", 8L,
                "mean", 3.75,
                "p50", 2L,
                "p75", 3L,
                "p90", 8L,
                "p95", 8L,
                "p99", 8L,
                "p999", 8L
            )),
            "createMillis", greaterThanOrEqualTo(0L),
            "computeMillis", greaterThanOrEqualTo(0L),
            "postProcessingMillis", greaterThanOrEqualTo(0L),
            "configuration", containsAllEntriesOf(MapUtil.map(
                "consecutiveIds", false,
                "includeIntermediateCommunities", false,
                "maxIterations", 10,
                "maxLevels", 10,
                "tolerance", 1e-4,
                "seedProperty", null,
                "relationshipWeightProperty", null
            ))
        )));
    }

    @Test
    void zeroCommunitiesInEmptyGraph() {
        runQuery("CALL db.createLabel('VeryTemp')");
        runQuery("CALL db.createRelationshipType('VERY_TEMP')");
        String query = GdsCypher
            .call()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .algo("louvain")
            .statsMode()
            .yields("communityCount");

        assertCypherResult(query, List.of(Map.of("communityCount", 0L)));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("org.neo4j.graphalgo.louvain.LouvainProcTest#graphVariations")
    void statsShouldNotHaveWriteProperties(GdsCypher.QueryBuilder queryBuilder, String testCaseName) {
        String query = queryBuilder
            .algo("louvain")
            .statsMode()
            .yields();

        List<String> forbiddenResultColumns = Arrays.asList(
            "writeMillis",
            "nodePropertiesWritten",
            "relationshipPropertiesWritten"
        );
        List<String> forbiddenConfigKeys = Collections.singletonList("writeProperty");
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
}
