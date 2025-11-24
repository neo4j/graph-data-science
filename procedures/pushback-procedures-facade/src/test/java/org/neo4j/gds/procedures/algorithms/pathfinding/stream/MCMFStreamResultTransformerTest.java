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
package org.neo4j.gds.procedures.algorithms.pathfinding.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.maxflow.FlowRelationship;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MaxFlowStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MCMFStreamResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamOnEmptyResult() {
        var idMock = mock(IdMap.class);
        when(idMock.nodeCount()).thenReturn(0L);

        var transformer = new MCMFStreamResultTransformer(idMock);

        var streamResult = transformer.apply(TimedAlgorithmResult.empty(CostFlowResult.EMPTY));
        assertThat(streamResult).isEmpty();
    }

    @Test
    void shouldTransformNonEmptyResult() {
        var idMock = mock(IdMap.class);
        when(idMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var flow = HugeObjectArray.newArray(FlowRelationship.class, 2);
        flow.set(0L, new FlowRelationship(0, 3, 4D));
        flow.set(1L, new FlowRelationship(3, 6, 4D));
        var flowResult = new FlowResult(flow, 4D);
        var costFlowResult  =new CostFlowResult(flowResult,0);
        var transformer = new MCMFStreamResultTransformer(idMock);


        var streamResult = transformer.apply(new TimedAlgorithmResult<>(costFlowResult, 1L)).toList();
        assertThat(streamResult.getFirst()).isEqualTo(new MaxFlowStreamResult(0,3,4D));
        assertThat(streamResult.getLast()).isEqualTo(new MaxFlowStreamResult(3,6,4D));

    }
}
