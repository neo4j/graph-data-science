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
import org.neo4j.gds.hdbscan.Labels;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HDBScanStatsResultTransformerTest {

        @Test
        void shouldTransform() {
        var config = Map.of("a", (Object) ("foo"));
        var result = mock(Labels.class);
        when(result.numberOfClusters()).thenReturn(20L);
        when(result.numberOfNoisePoints()).thenReturn(100L);


        var transformer = new HDBScanStatsResultTransformer(config,3L);

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.nodeCount()).isEqualTo(3L);
                assertThat(stats.numberOfClusters()).isEqualTo(20L);
                assertThat(stats.numberOfNoisePoints()).isEqualTo(100L);
                assertThat(stats.preProcessingMillis()).isEqualTo(0);
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.postProcessingMillis()).isEqualTo(0);
                assertThat(stats.configuration()).isEqualTo(config);
            });
    }

}
