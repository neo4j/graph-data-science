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
package org.neo4j.gds.procedures.algorithms.centrality.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.bridges.Bridge;
import org.neo4j.gds.bridges.BridgeResult;
import org.neo4j.gds.procedures.algorithms.centrality.BridgesStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class BridgesStreamResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var bridge = new Bridge(0,1,new long[]{10,9});
        var result = new BridgeResult(List.of(bridge));

        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new BridgesStreamResultTransformer(graphMock,false);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new BridgesStreamResult(0,1,null)
        );
        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verifyNoMoreInteractions(graphMock);

    }

    @Test
    void shouldTransformResultAndReportStats(){

        var bridge = new Bridge(0,1,new long[]{10,9});
        var result = new BridgeResult(List.of(bridge));

        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new BridgesStreamResultTransformer(graphMock,true);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new BridgesStreamResult(0,1,List.of(10L,9L))
        );
        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verifyNoMoreInteractions(graphMock);
    }

    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(0L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new BridgesStreamResultTransformer(graphMock,true);

        var stream = transformer.apply(new TimedAlgorithmResult<>(BridgeResult.EMPTY, -1));
        assertThat(stream).isEmpty();
        verifyNoMoreInteractions(graphMock);

    }

}