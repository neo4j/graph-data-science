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
package org.neo4j.graphalgo.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.impl.triangle.TriangleCountStreamConfig;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TriangleCountStatsProcTest extends TriangleBaseProcTest<TriangleCountStreamConfig> {

    @Override
    TriangleBaseProc<TriangleCountStreamConfig> newInstance() {
        return new TriangleCountStatsProc();
    }

    @Override
    TriangleCountStreamConfig newConfig() {
        return TriangleCountStreamConfig.of(
            getUsername(),
            Optional.empty(),
            Optional.empty(),
            CypherMapWrapper.empty()
        );
    }

    @Test
    void testStats() {
        String query = GdsCypher.call()
            .loadEverything(Orientation.UNDIRECTED)
            .algo("gds", "alpha", "triangleCount")
            .statsMode()
            .yields();

        runQueryWithRowConsumer(query, row -> {
            long createMillis = row.getNumber("createMillis").longValue();
            long computeMillis = row.getNumber("computeMillis").longValue();
            long nodeCount = row.getNumber("nodeCount").longValue();
            long triangleCount = row.getNumber("triangleCount").longValue();
            assertNotEquals(-1, createMillis);
            assertNotEquals(-1, computeMillis);
            assertEquals(3, triangleCount);
            assertEquals(9, nodeCount);
        });
    }
}
