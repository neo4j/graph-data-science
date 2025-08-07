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
package org.neo4j.gds.procedures.algorithms.pathfinding.stats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class BellmanFordStatsResultTransformerTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldTransformToStatsResult(boolean containsNegativeCycles) {
        var config = Map.<String, Object>of("foo", "bar");
        var transformer = new BellmanFordStatsResultTransformer(config);

        var timedResult = new TimedAlgorithmResult<>(
            new BellmanFordResult(
                mock(PathFindingResult.class),
                mock(PathFindingResult.class),
                containsNegativeCycles
            ),
            123L
        );

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.postProcessingMillis()).isZero();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.containsNegativeCycle()).isEqualTo(containsNegativeCycles);
    }

    @Test
    void shouldTransformEmptyResult() {
        var config = Map.<String, Object>of("foo", "bar");
        var transformer = new BellmanFordStatsResultTransformer(config);

        var timedResult = TimedAlgorithmResult.empty(BellmanFordResult.empty());

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isZero();
        assertThat(result.postProcessingMillis()).isZero();
        assertThat(result.configuration()).isEqualTo(config);
        assertThat(result.containsNegativeCycle()).isFalse();
    }

}
