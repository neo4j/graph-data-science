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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MCMFStatsResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamOnEmptyResult() {
        var configuration = Map.<String, Object>of("foo", "bar");


        var transformer = new MaxFlowStatsResultTransformer(configuration);

        var statsResult = transformer.apply(TimedAlgorithmResult.empty(FlowResult.EMPTY)).findFirst().orElseThrow();
        assertThat(statsResult.preProcessingMillis()).isZero();
        assertThat(statsResult.computeMillis()).isZero();
        assertThat(statsResult.totalFlow()).isZero();
        assertThat(statsResult.configuration()).isEqualTo(configuration);
    }

    @Test
    void shouldTransformNonEmptyResult() {
        var configuration = Map.<String, Object>of("foo", "bar");
        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new MaxFlowStatsResultTransformer(configuration);

        var flowResult = new FlowResult(null, 4D);

        var statsResult = transformer.apply(new TimedAlgorithmResult<>(flowResult, 1L)).findFirst().orElseThrow();
        assertThat(statsResult.preProcessingMillis()).isZero();
        assertThat(statsResult.computeMillis()).isEqualTo(1L);
        assertThat(statsResult.totalFlow()).isEqualTo(4D);
        assertThat(statsResult.configuration()).isEqualTo(configuration);
    }

}
