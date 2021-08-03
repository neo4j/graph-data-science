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

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

class LocalClusteringCoefficientStatsProcTest extends LocalClusteringCoefficientBaseProcTest<LocalClusteringCoefficientStatsConfig> {

    @Test
    void testStats() {
        var query = "CALL gds.localClusteringCoefficient.stats('g')";

        assertCypherResult(query, List.of(Map.of(
            "averageClusteringCoefficient", closeTo(expectedAverageClusteringCoefficient() / 5, 1e-10),
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Test
    void testStatsSeeded() {
        var query = "CALL gds.localClusteringCoefficient.stats('g', { triangleCountProperty: 'seed'})";

        assertCypherResult(query, List.of(Map.of(
            "averageClusteringCoefficient", closeTo(expectedAverageClusteringCoefficientSeeded() / 5, 1e-10),
            "nodeCount", 5L,
            "createMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Override
    public Class<? extends AlgoBaseProc<LocalClusteringCoefficient, LocalClusteringCoefficient.Result, LocalClusteringCoefficientStatsConfig>> getProcedureClazz() {
        return LocalClusteringCoefficientStatsProc.class;
    }

    @Override
    public LocalClusteringCoefficientStatsConfig createConfig(CypherMapWrapper mapWrapper) {
        return LocalClusteringCoefficientStatsConfig.of(
            getUsername(),
            Optional.empty(),
            Optional.empty(),
            mapWrapper
        );
    }
}
