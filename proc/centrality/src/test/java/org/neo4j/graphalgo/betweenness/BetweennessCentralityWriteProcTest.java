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
package org.neo4j.graphalgo.betweenness;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.WritePropertyConfigTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BetweennessCentralityWriteProcTest
    extends BetweennessCentralityProcTest<BetweennessCentralityWriteConfig>
    implements WritePropertyConfigTest<BetweennessCentrality, BetweennessCentralityWriteConfig, BetweennessCentrality> {

    private static final String WRITE_PROPERTY = "centrality";

    @Override
    public Class<? extends AlgoBaseProc<BetweennessCentrality, BetweennessCentrality, BetweennessCentralityWriteConfig>> getProcedureClazz() {
        return BetweennessCentralityWriteProc.class;
    }

    @Override
    public BetweennessCentralityWriteConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityWriteConfig.of("", Optional.empty(), Optional.empty(), mapWrapper);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("writeProperty")) {
            return mapWrapper.withString("writeProperty", WRITE_PROPERTY);
        }
        return mapWrapper;
    }

    @Test
    void testWrite() {
        String query = GdsCypher
            .call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("betweenness")
            .writeMode()
            .addParameter("writeProperty", WRITE_PROPERTY)
            .addParameter("concurrency", 1)
            .yields(
                "nodePropertiesWritten",
                "createMillis",
                "computeMillis",
                "writeMillis",
                "minCentrality",
                "maxCentrality",
                "sumCentrality"
            );

        runQueryWithRowConsumer(query, row -> {
            assertEquals(5L, row.getNumber("nodePropertiesWritten"));

            assertNotEquals(-1L, row.getNumber("createMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("writeMillis"));

            assertEquals(0D, row.getNumber("minCentrality").doubleValue(), 1E-1);
            assertEquals(4D, row.getNumber("maxCentrality").doubleValue(), 1E-1);
            assertEquals(10D, row.getNumber("sumCentrality").doubleValue(), 1E-1);
        });
    }

}