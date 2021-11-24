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
package org.neo4j.gds.conductance;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.conductance.Conductance;
import org.neo4j.gds.impl.conductance.ConductanceStreamConfig;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ConductanceStreamProcTest extends ConductanceProcTest<ConductanceStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<Conductance, Conductance.Result, ConductanceStreamConfig>> getProcedureClazz() {
        return ConductanceStreamProc.class;
    }

    @Override
    public ConductanceStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return ConductanceStreamConfig.of(
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Test
    void testStream() {
        String streamQuery = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("gds.alpha.conductance")
            .streamMode()
            .addParameter("communityProperty", "community")
            .yields();

        var expected = Map.of(
            0L, 4.0 / (4.0 + 2.0),
            1L, 4.0 / (4.0 + 1.0)
        );
        runQueryWithRowConsumer(streamQuery, row -> {
            long community = row.getNumber("community").longValue();
            double conductance = row.getNumber("conductance").doubleValue();
            assertThat(conductance).isCloseTo(expected.get(community), Offset.offset(0.0001));
        });

    }
}
