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
package org.neo4j.graphalgo.betweenness;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicDoubleArray;

import java.util.Optional;

class BetweennessCentralityStreamProcTest extends BetweennessCentralityProcTest<BetweennessCentralityStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityStreamConfig>> getProcedureClazz() {
        return BetweennessCentralityStreamProc.class;
    }

    @Override
    public BetweennessCentralityStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityStreamConfig.of("",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Test
    void testStream() {
        var query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.betweenness")
            .streamMode()
            .yields();

        assertCypherResult(query, expected);
    }

    @Test
    void shouldValidateSampleSize() {
        var query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.betweenness")
            .streamMode()
            .addParameter("samplingSize", -42)
            .yields();

        assertError(query, "Configuration parameter 'samplingSize' must be a positive number, got -42.");
    }

}
