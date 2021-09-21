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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

class FastRPStatsProcTest extends FastRPProcTest<FastRPStatsConfig> {

    @Override
    GdsCypher.ExecutionModes mode() {
        return GdsCypher.ExecutionModes.STATS;
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPStatsConfig>> getProcedureClazz() {
        return FastRPStatsProc.class;
    }

    @Override
    public FastRPStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return FastRPStatsConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void testStats() {
        var query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("fastRP")
            .statsMode()
            .addParameter("embeddingDimension", 2)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Test
    void shouldNotCrashWithFeatureProperties() {
        var query = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL")
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0D))
            .algo("fastRP")
            .statsMode()
            .addParameter("embeddingDimension", 4)
            .addParameter("propertyRatio", 0.5)
            .addParameter("featureProperties", List.of("f1", "f2"))
            .yields();

        runQuery(query);
    }
}
