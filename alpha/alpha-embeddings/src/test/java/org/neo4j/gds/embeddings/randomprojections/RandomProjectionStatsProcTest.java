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
package org.neo4j.gds.embeddings.randomprojections;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

class RandomProjectionStatsProcTest extends RandomProjectionProcTest<RandomProjectionStatsConfig> {

    @Override
    public Class<? extends AlgoBaseProc<RandomProjection, RandomProjection, RandomProjectionStatsConfig>> getProcedureClazz() {
        return RandomProjectionStatsProc.class;
    }

    @Override
    public RandomProjectionStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return RandomProjectionStatsConfig.of(getUsername(), Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Test
    void testStats() {
        var query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .statsMode()
            .addParameter("embeddingSize", 2)
            .addParameter("maxIterations", 2)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }
}
