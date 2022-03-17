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
package org.neo4j.gds.beta.closeness;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.CypherMapWrapper;

class ClosenessCentralityStreamProcTest extends ClosenessCentralityProcTest<ClosenessCentralityStreamConfig> {
    @Override
    public Class<? extends AlgoBaseProc<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityStreamConfig, ?>> getProcedureClazz() {
        return ClosenessCentralityStreamProc.class;
    }

    @Override
    public ClosenessCentralityStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ClosenessCentralityStreamConfig.of(mapWrapper);
    }

    @Test
    void testClosenessStream() {
        loadGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.closeness")
            .streamMode()
            .yields("nodeId", "score");

        assertCypherResult(query, expectedCentralityResult);
    }
}
