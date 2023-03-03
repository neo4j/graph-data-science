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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LocalClusteringCoefficientStreamProcTest extends LocalClusteringCoefficientBaseProcTest<LocalClusteringCoefficientStreamConfig> {

    @Test
    void testStreaming() {
        var query = "CALL gds.localClusteringCoefficient.stream('g')";

        Map<Long, Double> actualResult = new HashMap<>();
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double localClusteringCoefficient = row.getNumber("localClusteringCoefficient").doubleValue();
            actualResult.put(nodeId, localClusteringCoefficient);
        });

        assertStreamResult(actualResult, expectedResult);
    }

    @Test
    void testStreamingSeeded() {
        var query = "CALL gds.localClusteringCoefficient.stream('g', { triangleCountProperty: 'seed'})";

        Map<Long, Double> actualResult = new HashMap<>();
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double localClusteringCoefficient = row.getNumber("localClusteringCoefficient").doubleValue();
            actualResult.put(nodeId, localClusteringCoefficient);
        });

        assertStreamResult(actualResult, expectedResultWithSeeding);
    }

    private void assertStreamResult(Map<Long, Double> actualResult, Map<String, Double> expectedResult) {
        actualResult.forEach(
            (nodeId, coefficient) -> {
                runQueryWithRowConsumer(String.format(
                    "MATCH (n) WHERE id(n) = %d RETURN n.name AS name",
                    nodeId
                ), row -> {
                    String name = row.getString("name");
                    assertThat(expectedResult.get(name)).isEqualTo(coefficient);
                });
            }
        );
    }

    @Override
    public Class<? extends AlgoBaseProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStreamConfig, ?>> getProcedureClazz() {
        return LocalClusteringCoefficientStreamProc.class;
    }

    @Override
    public LocalClusteringCoefficientStreamConfig createConfig(CypherMapWrapper mapWrapper) {
        return LocalClusteringCoefficientStreamConfig.of(mapWrapper);
    }
}
