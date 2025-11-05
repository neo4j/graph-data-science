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
package org.neo4j.gds.procedures.algorithms.community.stats;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class K1ColoringStatsResultTransformerTest {

    @Test
    void shouldTransformResultButDoNotComputeUsedColors(){

        var config = Map.of("a",(Object)("foo"));
        var result = mock(K1ColoringResult.class);
        when(result.didConverge()).thenReturn(true);
        when(result.ranIterations()).thenReturn(100L);

        var bitset = new BitSet(10);
        bitset.set(2);
        bitset.set(4);
        when(result.usedColors()).thenReturn(bitset);
        when(result.colors()).thenReturn(HugeLongArray.of(1,2,3));

        var transformer = new K1ColoringStatsResultTransformer(config,false);

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isEqualTo(0);
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.nodeCount()).isEqualTo(3);
                assertThat(stats.colorCount()).isEqualTo(0);
                assertThat(stats.ranIterations()).isEqualTo(100);
                assertThat(stats.didConverge()).isTrue();
                assertThat(stats.configuration()).isEqualTo(config);
            });

    }

    @Test
    void shouldTransformResultAndComputeUsedColors() {

        var config = Map.of("a", (Object) ("foo"));
        var result = mock(K1ColoringResult.class);
        when(result.didConverge()).thenReturn(true);
        when(result.ranIterations()).thenReturn(100L);

        var bitset = new BitSet(10);
        bitset.set(2);
        bitset.set(4);
        when(result.usedColors()).thenReturn(bitset);
        when(result.colors()).thenReturn(HugeLongArray.of(1, 2, 3));

        var transformer = new K1ColoringStatsResultTransformer(config, true);

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.preProcessingMillis()).isEqualTo(0);
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.nodeCount()).isEqualTo(3);
                assertThat(stats.colorCount()).isEqualTo(2);
                assertThat(stats.ranIterations()).isEqualTo(100);
                assertThat(stats.didConverge()).isTrue();
                assertThat(stats.configuration()).isEqualTo(config);
            });
    }

}
