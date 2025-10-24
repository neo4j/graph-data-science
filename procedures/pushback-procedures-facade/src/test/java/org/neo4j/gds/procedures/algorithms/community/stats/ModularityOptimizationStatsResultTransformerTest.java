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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ModularityOptimizationStatsResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var config = Map.of("a",(Object)("foo"));
        var result = mock(ModularityOptimizationResult.class);
        HugeLongArray hugeLongArray = HugeLongArray.of(0L, 20L, 10L, 20L, 10L, 10L);

        when(result.communityIdLookup()).thenReturn(hugeLongArray::get);
        when(result.didConverge()).thenReturn(true);
        when(result.modularity()).thenReturn(55d);
        when(result.nodeCount()).thenReturn(6L);
        when(result.ranIterations()).thenReturn(1000);

        var instructions = mock(StatisticsComputationInstructions.class);
        when(instructions.computeCountAndDistribution()).thenReturn(true);
        var transformer = new ModularityOptimizationStatsResultTransformer(config,instructions,new Concurrency(1));

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(
                stats ->{
                    assertThat(stats.ranIterations()).isEqualTo(1000);
                    assertThat(stats.computeMillis()).isEqualTo(10);
                    assertThat(stats.communityCount()).isEqualTo(3);
                    assertThat(stats.postProcessingMillis()).isNotNegative();
                    assertThat(stats.communityDistribution()).containsKey("p99");
                    assertThat(stats.modularity()).isEqualTo(55);
                    assertThat(stats.didConverge()).isTrue();
                }
            );

    }

}
