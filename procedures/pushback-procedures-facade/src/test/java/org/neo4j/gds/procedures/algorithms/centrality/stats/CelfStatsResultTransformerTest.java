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
package org.neo4j.gds.procedures.algorithms.centrality.stats;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CelfStatsResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var result = mock(CELFResult.class);
        when(result.totalSpread()).thenReturn(55d);

        var config = Map.of("a",(Object)("foo"));

        var transformer = new CelfStatsResultTransformer(
            20,
            config
        );

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(result, 10));

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.configuration()).isEqualTo(config);
                assertThat(stats.totalSpread()).isEqualTo(55);
                assertThat(stats.nodeCount()).isEqualTo(20);
            });
    }

    @Test
    void shouldTransformEmptyResult(){

        var config = Map.of("a",(Object)("foo"));

        var transformer = new CelfStatsResultTransformer(
            0,
            config
        );

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(
            CELFResult.EMPTY,
            10)
        );

        assertThat(statsResult.findFirst().orElseThrow())
            .satisfies(stats -> {
                assertThat(stats.computeMillis()).isEqualTo(10);
                assertThat(stats.configuration()).isEqualTo(config);
                assertThat(stats.nodeCount()).isEqualTo(0);
                assertThat(stats.totalSpread()).isEqualTo(0);
            });
    }

}
