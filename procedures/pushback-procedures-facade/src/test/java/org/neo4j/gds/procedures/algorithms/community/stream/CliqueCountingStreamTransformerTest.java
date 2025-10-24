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
import org.neo4j.gds.cliqueCounting.CliqueCountingResult;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class CliqueCountingStreamTransformerTest {

    @Test
    void shouldTransformResult(){

        var result = mock(CliqueCountingResult.class);
        when(result.perNodeCount()).thenReturn(HugeObjectArray.of(new long[]{1L},new long[]{1L},new long[]{1L}));
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(3L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new CliqueCountingStreamTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result,-1));
        assertThat(stream).containsExactlyInAnyOrder(
            new CliqueCountingStreamResult(0, List.of(1L)),
            new CliqueCountingStreamResult(1, List.of(1L)),
            new CliqueCountingStreamResult(2, List.of(1L))
        );

        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verify(graphMock, times(1)).toOriginalNodeId(2L);
        verify(graphMock, times(1)).nodeCount();

        verifyNoMoreInteractions(graphMock);

    }


    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new CliqueCountingStreamTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(CliqueCountingResult.EMPTY,-1));
        assertThat(stream).isEmpty();

        verifyNoMoreInteractions(graphMock);

    }

}
