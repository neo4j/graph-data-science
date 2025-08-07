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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.spanningtree.SpanningTree;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SpanningTreeStatsResultTransformerTest {

    @Test
    void shouldTransformToStatsResultWithConfig() {
        var configuration = Map.<String, Object>of("foo", "bar");
        var parentArray = mock(HugeLongArray.class);
        var spanningTree = new SpanningTree(
            1L,
            3,
            2,
            parentArray,
            n -> 1.5,
            4.2
        );
        var timedResult = new TimedAlgorithmResult<>(spanningTree, 123L);
        var transformer = new SpanningTreeStatsResultTransformer(configuration);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(123L);
        assertThat(result.effectiveNodeCount()).isEqualTo(2);
        assertThat(result.totalWeight()).isEqualTo(4.2);
        assertThat(result.configuration()).isEqualTo(configuration);
    }

    @Test
    void shouldTransformToStatsResultWithEmptyConfig() {
        var parentArray = mock(HugeLongArray.class);
        var spanningTree = new SpanningTree(
            0L,
            0,
            0,
            parentArray,
            n -> 0.0,
            0.0
        );
        var timedResult = new TimedAlgorithmResult<>(spanningTree, 0L);
        var transformer = new SpanningTreeStatsResultTransformer(Collections.emptyMap());

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isZero();
        assertThat(result.effectiveNodeCount()).isZero();
        assertThat(result.totalWeight()).isZero();
        assertThat(result.configuration()).isEmpty();
    }

    @Test
    void shouldTransformToStatsResultWithNullConfig() {
        var parentArray = mock(HugeLongArray.class);
        var spanningTree = new SpanningTree(
            2L,
            1,
            1,
            parentArray,
            n -> 2.5,
            2.5
        );
        var timedResult = new TimedAlgorithmResult<>(spanningTree, 42L);
        var transformer = new SpanningTreeStatsResultTransformer(null);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isEqualTo(42L);
        assertThat(result.effectiveNodeCount()).isEqualTo(1);
        assertThat(result.totalWeight()).isEqualTo(2.5);
        assertThat(result.configuration()).isNull();
    }

    @Test
    void shouldTransformEmptySpanningTree() {
        var configuration = Map.<String, Object>of("x", 1);
        var transformer = new SpanningTreeStatsResultTransformer(configuration);

        var timedResult = TimedAlgorithmResult.empty(SpanningTree.EMPTY);

        var resultStream = transformer.apply(timedResult);
        var result = resultStream.findFirst().orElseThrow();

        assertThat(result.preProcessingMillis()).isZero();
        assertThat(result.computeMillis()).isZero();
        assertThat(result.effectiveNodeCount()).isZero();
        assertThat(result.totalWeight()).isZero();
        assertThat(result.configuration()).isEqualTo(configuration);
    }
}
