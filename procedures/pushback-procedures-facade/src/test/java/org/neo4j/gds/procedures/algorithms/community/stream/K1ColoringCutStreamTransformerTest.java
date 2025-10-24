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
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class K1ColoringCutStreamTransformerTest {

    @Test
    void shouldTransformResult(){

        var result = mock(K1ColoringResult.class);
        when(result.colors()).thenReturn(HugeLongArray.of(0L,2L,1L,2L,1L,1L));
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(6L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new K1ColoringCutStreamTransformer(graphMock,new Concurrency(1), Optional.of(2L));

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new K1ColoringStreamResult(1,2),
            new K1ColoringStreamResult(2,1),
            new K1ColoringStreamResult(3,2),
            new K1ColoringStreamResult(4,1),
            new K1ColoringStreamResult(5,1)
        );

        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verify(graphMock, times(1)).toOriginalNodeId(2L);
        verify(graphMock, times(1)).toOriginalNodeId(3L);
        verify(graphMock, times(1)).toOriginalNodeId(4L);
        verify(graphMock, times(1)).toOriginalNodeId(5L);
        verifyNoMoreInteractions(graphMock);

    }

    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(6L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new K1ColoringCutStreamTransformer(graphMock,new Concurrency(1), Optional.of(2L));

        var stream = transformer.apply(new TimedAlgorithmResult<>(K1ColoringResult.EMPTY, -1));
        assertThat(stream).isEmpty();

        verifyNoMoreInteractions(graphMock);

    }

}
