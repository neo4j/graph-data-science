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
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.procedures.algorithms.community.TriangleCountStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.triangle.TriangleCountResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TriangleCountStreamTransformerTest {

    @Test
    void shouldTransformResult(){

        var result = mock(TriangleCountResult.class);
        var localTriangles = HugeAtomicLongArray.of(2, ParalleLongPageCreator.passThrough(new Concurrency(1)));
        localTriangles.set(0,10L);
        localTriangles.set(1,20L);


        when(result.localTriangles()).thenReturn(localTriangles);
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(2L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new TriangleCountStreamTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new TriangleCountStreamResult(0, 10L),
            new TriangleCountStreamResult(1, 20L)
        );

        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verify(graphMock, times(1)).nodeCount();

        verifyNoMoreInteractions(graphMock);

    }


    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new TriangleCountStreamTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(TriangleCountResult.EMPTY,-1L));
        assertThat(stream).isEmpty();

        verifyNoMoreInteractions(graphMock);

    }

}
