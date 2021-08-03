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
package org.neo4j.graphalgo.degree;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.degree.DegreeCentrality;
import org.neo4j.gds.degree.DegreeCentralityStatsConfig;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DegreeCentralityStatsProcTest extends DegreeCentralityProcTest<DegreeCentralityStatsConfig> {

    @Override
    public Class<? extends AlgoBaseProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStatsConfig>> getProcedureClazz() {
        return DegreeCentralityStatsProc.class;
    }

    @Override
    public DegreeCentralityStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return DegreeCentralityStatsConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Test
    void testStats() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("degree")
            .statsMode()
            .yields("centralityDistribution", "createMillis", "computeMillis", "postProcessingMillis");

        runQueryWithRowConsumer(query, row -> {
            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(0.0, centralityDistribution.get("min"));
            assertEquals(3.0, (double) centralityDistribution.get("max"), 1e-4);

            assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("postProcessingMillis").longValue()));
        });
    }
}
