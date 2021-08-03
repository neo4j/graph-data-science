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
package org.neo4j.gds.betweenness;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;

import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class BetweennessCentralityStatsProcTest extends BetweennessCentralityProcTest<BetweennessCentralityStatsConfig> {
    @Override
    public Class<? extends AlgoBaseProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityStatsConfig>> getProcedureClazz() {
        return BetweennessCentralityStatsProc.class;
    }

    @Override
    public BetweennessCentralityStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityStatsConfig.of("",
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
            .algo("betweenness")
            .statsMode()
            .yields("centralityDistribution", "createMillis", "computeMillis", "postProcessingMillis", "minimumScore", "maximumScore", "scoreSum");

        runQueryWithRowConsumer(query, row -> {
            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(0.0, centralityDistribution.get("min"));
            assertEquals(4.0, (double) centralityDistribution.get("max"), 1e-4);
            assertEquals(10.0, row.getNumber("scoreSum"));


            assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("postProcessingMillis").longValue()));
        });
    }

    @Test
    void testStatsWithDeprecatedFields() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("betweenness")
            .statsMode()
            .yields("createMillis", "computeMillis", "postProcessingMillis", "minimumScore", "maximumScore", "scoreSum");

        runQueryWithRowConsumer(query, row -> {
            assertEquals(0.0, row.getNumber("minimumScore"));
            assertEquals(4.0, row.getNumber("maximumScore"));
            assertEquals(10.0, row.getNumber("scoreSum"));


            assertThat(-1L, lessThan(row.getNumber("createMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("computeMillis").longValue()));
            assertThat(-1L, lessThan(row.getNumber("postProcessingMillis").longValue()));
        });
    }
}
