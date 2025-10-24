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
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KMeansStatsResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var config = Map.of("a",(Object)("foo"));
        var result = new KmeansResult(
            HugeIntArray.of(0,0,1,1,1),
            null,
            new double[][]{new double[]{1}, new double[]{2}},
            2.0,
            null,
            4.0
        );

        var instructions = mock(KMeansStatsResultTransformer.KmeansStatisticsComputationInstructions.class);
        when(instructions.computeCountAndDistribution()).thenReturn(true);
        when(instructions.shouldComputeListOfCentroids()).thenReturn(true);
        var transformer = new KMeansStatsResultTransformer(config,instructions,new Concurrency(1));

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(
                stats ->{
                    assertThat(stats.averageDistanceToCentroid()).isEqualTo(2.0);
                    assertThat(stats.centroids()).isEqualTo(List.of(List.of(1d), List.of(2d)));
                    assertThat(stats.computeMillis()).isEqualTo(10);
                    assertThat(stats.averageSilhouette()).isEqualTo(4.0);
                    assertThat(stats.communityDistribution()).containsKey("p99");
                }
            );

    }

}
