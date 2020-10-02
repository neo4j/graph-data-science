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
package org.neo4j.graphalgo.wcc;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.graphalgo.assertj.ConditionFactory.containsAllEntriesOf;

class WccStatsProcTest extends WccProcTest<WccStatsConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Wcc, DisjointSetStruct, WccStatsConfig>> getProcedureClazz() {
        return WccStatsProc.class;
    }

    @Override
    public WccStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return WccStatsConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void yields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("wcc")
            .statsMode()
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "componentCount", 3L,
            "componentDistribution", Map.of(
                "min", 1L,
                "max", 7L,
                "mean", 3.3333333333333335D,
                "p50", 2L,
                "p75", 2L,
                "p90", 7L,
                "p95", 7L,
                "p99", 7L,
                "p999", 7L
            ),
            "createMillis", greaterThanOrEqualTo(0L),
            "computeMillis", greaterThanOrEqualTo(0L),
            "postProcessingMillis", greaterThanOrEqualTo(0L),
            "configuration", containsAllEntriesOf(MapUtil.map(
                "consecutiveIds", false,
                "threshold", 0D,
                "seedProperty", null,
                "relationshipWeightProperty", null
            ))
        )));
    }

    @Test
    void zeroComponentsInEmptyGraph() {
        runQuery("CREATE (:VeryTemp)-[:VERY_TEMP]->(:VeryTemp)");
        runQuery("MATCH (a:VeryTemp)-[r:VERY_TEMP]->(b:VeryTemp) DELETE a, r, b");
        String query = GdsCypher
            .call()
            .withNodeLabel("VeryTemp")
            .withRelationshipType("VERY_TEMP")
            .algo("wcc")
            .statsMode()
            .yields("componentCount");

        assertCypherResult(query, List.of(MapUtil.map("componentCount", 0L)));
    }
}
