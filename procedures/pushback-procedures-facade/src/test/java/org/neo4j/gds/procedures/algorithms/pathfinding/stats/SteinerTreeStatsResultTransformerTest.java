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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.steiner.SteinerTreeResult;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SteinerTreeStatsResultTransformerTest {

    @Test
    void shouldTransformToStatsResultWithConfig() {
        var config = Map.<String, Object>of("foo", "bar");
        var parentArray = mock(HugeLongArray.class);
        var costArray = mock(HugeDoubleArray.class);
        var steinerTreeResult = new SteinerTreeResult(
            parentArray,
            costArray,
            42.0,
            5,
            3
        );
        var timedResult = new TimedAlgorithmResult<>(steinerTreeResult, 123L);
        var transformer = new SteinerTreeStatsResultTransformer(config);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.effectiveNodeCount()).isEqualTo(5);
        assertThat(result.effectiveTargetNodesCount()).isEqualTo(3);
        assertThat(result.totalWeight()).isEqualTo(42.0);
        assertThat(result.configuration()).isEqualTo(config);
    }

    @Test
    void shouldTransformToStatsResultWithEmptyConfig() {
        var parentArray = mock(HugeLongArray.class);
        var costArray = mock(HugeDoubleArray.class);
        var steinerTreeResult = new SteinerTreeResult(
            parentArray,
            costArray,
            0.0,
            0,
            0
        );
        var timedResult = new TimedAlgorithmResult<>(steinerTreeResult, 0L);
        var transformer = new SteinerTreeStatsResultTransformer(Collections.emptyMap());

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isZero();
        assertThat(result.effectiveNodeCount()).isZero();
        assertThat(result.effectiveTargetNodesCount()).isZero();
        assertThat(result.totalWeight()).isZero();
        assertThat(result.configuration()).isEmpty();
    }

    @Test
    void shouldTransformToStatsResultWithNullConfig() {
        var parentArray = mock(HugeLongArray.class);
        var costArray = mock(HugeDoubleArray.class);
        var steinerTreeResult = new SteinerTreeResult(
            parentArray,
            costArray,
            1.5,
            2,
            1
        );
        var timedResult = new TimedAlgorithmResult<>(steinerTreeResult, 42L);
        var transformer = new SteinerTreeStatsResultTransformer(null);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(42L);
        assertThat(result.effectiveNodeCount()).isEqualTo(2);
        assertThat(result.effectiveTargetNodesCount()).isEqualTo(1);
        assertThat(result.totalWeight()).isEqualTo(1.5);
        assertThat(result.configuration()).isNull();
    }

    @Test
    void shouldTransformEmptySteinerTree() {
        var config = Map.<String, Object>of("x", 1);
        var transformer = new SteinerTreeStatsResultTransformer(config);

        var timedResult = TimedAlgorithmResult.empty(SteinerTreeResult.EMPTY);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isZero();
        assertThat(result.effectiveNodeCount()).isZero();
        assertThat(result.effectiveTargetNodesCount()).isZero();
        assertThat(result.totalWeight()).isZero();
        assertThat(result.configuration()).isEqualTo(config);
    }
}
