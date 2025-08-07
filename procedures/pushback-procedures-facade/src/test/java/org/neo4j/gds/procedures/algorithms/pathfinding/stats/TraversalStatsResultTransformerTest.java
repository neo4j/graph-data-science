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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class TraversalStatsResultTransformerTest {

    @Mock
    private HugeLongArray traversalResultMock;

    @Test
    void shouldTransformToStatsResultWithNonEmptyConfig() {
        var config = Map.<String, Object>of("key", "value");
        var transformer = new TraversalStatsResultTransformer(() -> config);

        var timedResult = new TimedAlgorithmResult<>(traversalResultMock, 100L);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(100L);
        assertThat(result.postProcessingMillis()).isZero();
        assertThat(result.configuration()).isEqualTo(config);
    }

    @Test
    void shouldTransformToStatsResultWithEmptyConfig() {
        var config = Collections.<String, Object>emptyMap();
        var transformer = new TraversalStatsResultTransformer(() -> config);

        var timedResult = new TimedAlgorithmResult<>(traversalResultMock, 0L);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isZero();
        assertThat(result.postProcessingMillis()).isZero();
        assertThat(result.configuration()).isEmpty();
    }

    @Test
    void shouldHandleNullConfigSupplier() {
        var transformer = new TraversalStatsResultTransformer(() -> null);

        var timedResult = new TimedAlgorithmResult<>(traversalResultMock, 50L);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.configuration()).isNull();
    }

    @Test
    void shouldHandleLargeComputeMillis() {
        var config = new HashMap<String, Object>();
        var transformer = new TraversalStatsResultTransformer(() -> config);

        var timedResult = new TimedAlgorithmResult<>(traversalResultMock, Long.MAX_VALUE);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.computeMillis()).isEqualTo(Long.MAX_VALUE);
    }
}
