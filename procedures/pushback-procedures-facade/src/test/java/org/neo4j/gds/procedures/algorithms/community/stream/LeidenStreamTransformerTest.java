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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.leiden.LeidenDendrogramManager;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.procedures.algorithms.community.LeidenStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class LeidenStreamTransformerTest {

    @Test
    void shouldTransformResultWithoutIntermediate(){
        var current = HugeLongArray.of(0L,20L,10L,20L,10L,10L);
        var dendrogramManager = mock(LeidenDendrogramManager.class);
        when(dendrogramManager.getCurrent()).thenReturn(current);
        when(dendrogramManager.getAllDendrograms()).thenReturn(new HugeLongArray[]{HugeLongArray.of(100), HugeLongArray.of(20)});
        var result = new LeidenResult(current,10,false,dendrogramManager,null,0);
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(6L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new LeidenStreamTransformer(graphMock,new Concurrency(1), Optional.of(2L),true,false);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new LeidenStreamResult(1,null,0),
            new LeidenStreamResult(2,null,1),
            new LeidenStreamResult(3,null,0),
            new LeidenStreamResult(4,null,1),
            new LeidenStreamResult(5,null,1)
        );

        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verify(graphMock, times(1)).toOriginalNodeId(2L);
        verify(graphMock, times(1)).toOriginalNodeId(3L);
        verify(graphMock, times(1)).toOriginalNodeId(4L);
        verify(graphMock, times(1)).toOriginalNodeId(5L);
        verifyNoMoreInteractions(graphMock);

    }

    @Test
    void shouldTransformResultWithIntermediate(){
        var current= HugeLongArray.of(10);
        var dendrogramManager = mock(LeidenDendrogramManager.class);
        when(dendrogramManager.getCurrent()).thenReturn(current);
        when(dendrogramManager.getAllDendrograms()).thenReturn(new HugeLongArray[]{HugeLongArray.of(100), HugeLongArray.of(20)});

        var result = new LeidenResult(current,2,false,dendrogramManager,null,0);

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(1L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new LeidenStreamTransformer(graphMock,new Concurrency(1), Optional.empty(),false,true);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new LeidenStreamResult(0, List.of(100L,20L),10)
        );
    }

    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(6L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new LeidenStreamTransformer(graphMock,new Concurrency(1), Optional.of(2L),true,false);

        var stream = transformer.apply(new TimedAlgorithmResult<>(LeidenResult.EMPTY, -1));
        assertThat(stream).isEmpty();

        verifyNoMoreInteractions(graphMock);

    }

}
