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
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DegreeCentralityStreamProcTest extends DegreeCentralityProcTest<DegreeCentralityStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityStreamConfig>> getProcedureClazz() {
        return DegreeCentralityStreamProc.class;
    }

    @Override
    public DegreeCentralityStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return DegreeCentralityStreamConfig.of(
            "",
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }

    @Test
    void testStream() {
        String streamQuery = GdsCypher.call()
            .explicitCreation(GRAPH_NAME)
            .algo("degree")
            .streamMode()
            .yields();

        Map<Long, Double> expected = Map.of(
            idFunction.of("a"), 0.0D,
            idFunction.of("b"), 1.0D,
            idFunction.of("c"), 1.0D,
            idFunction.of("d"), 2.0D,
            idFunction.of("e"), 3.0D,
            idFunction.of("f"), 2.0D,
            idFunction.of("g"), 0.0D,
            idFunction.of("h"), 0.0D,
            idFunction.of("i"), 0.0D,
            idFunction.of("j"), 0.0D
        );
        runQueryWithRowConsumer(streamQuery, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double score = row.getNumber("score").doubleValue();
            assertEquals(expected.get(nodeId), score);
        });

    }
}
