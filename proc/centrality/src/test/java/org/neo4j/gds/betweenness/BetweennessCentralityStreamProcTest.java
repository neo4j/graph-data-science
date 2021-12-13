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
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;

class BetweennessCentralityStreamProcTest extends BetweennessCentralityProcTest<BetweennessCentralityStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<BetweennessCentrality, HugeAtomicDoubleArray, BetweennessCentralityStreamConfig, ?>> getProcedureClazz() {
        return BetweennessCentralityStreamProc.class;
    }

    @Override
    public BetweennessCentralityStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return BetweennessCentralityStreamConfig.of(mapWrapper);
    }

    @Test
    void testStream() {
        loadGraph(DEFAULT_GRAPH_NAME);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.betweenness")
            .streamMode()
            .yields();

        assertCypherResult(query, expected);
    }

    @Test
    void shouldValidateSampleSize() {
        loadGraph(DEFAULT_GRAPH_NAME);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.betweenness")
            .streamMode()
            .addParameter("samplingSize", -42)
            .yields();

        assertError(query, "Configuration parameter 'samplingSize' must be a positive number, got -42.");
    }

}
