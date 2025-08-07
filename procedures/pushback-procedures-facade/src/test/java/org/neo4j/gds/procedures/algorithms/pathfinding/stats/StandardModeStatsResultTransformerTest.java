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
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class StandardModeStatsResultTransformerTest {

    @Test
    void shouldTransformToStatsResultWithConfig() {
        Supplier<Map<String, Object>> configSupplier = () -> Map.<String, Object>of("foo", "bar");
        var transformer = new StandardModeStatsResultTransformer<>(configSupplier);

        var timedResult = new TimedAlgorithmResult<>(new Object(), 123L);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis).isZero();
        assertThat(result.computeMillis).isEqualTo(123L);
        assertThat(result.configuration).isEqualTo(Map.<String, Object>of("foo", "bar"));
    }

    @Test
    void shouldTransformToStatsResultWithEmptyConfig() {
        Supplier<Map<String, Object>> configSupplier = Collections::emptyMap;
        var transformer = new StandardModeStatsResultTransformer<String>(configSupplier);

        var timedResult = TimedAlgorithmResult.empty("foo");

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis).isZero();
        assertThat(result.computeMillis).isZero();
        assertThat(result.configuration).isEmpty();
    }

    @Test
    void shouldTransformToStatsResultWithNullConfig() {
        Supplier<Map<String, Object>> configSupplier = () -> null;
        var transformer = new StandardModeStatsResultTransformer<>(configSupplier);

        var timedResult = new TimedAlgorithmResult<>(new Object(), 42L);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis).isZero();
        assertThat(result.computeMillis).isEqualTo(42L);
        assertThat(result.configuration).isNull();
    }
}
