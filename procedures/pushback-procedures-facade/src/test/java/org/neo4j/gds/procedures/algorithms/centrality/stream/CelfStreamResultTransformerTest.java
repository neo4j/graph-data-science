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

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.influenceMaximization.CELFResult;
import org.neo4j.gds.procedures.algorithms.centrality.CELFStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class CelfStreamResultTransformerTest {

    @Test
    void shouldTransformResult(){
        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var map = new LongDoubleScatterMap(10);
        map.put(2,33.0);
        map.put(4,45.0);
        var result = new CELFResult(map);
        var transformer = new CelfStreamResultTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new CELFStreamResult(2,33.0),
            new CELFStreamResult(4,45.0)
        );

        verify(graphMock, times(1)).toOriginalNodeId(2L);
        verify(graphMock, times(1)).toOriginalNodeId(4L);
        verifyNoMoreInteractions(graphMock);

    }

    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);

        var transformer = new CelfStreamResultTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(CELFResult.EMPTY, -1));
        assertThat(stream).isEmpty();

        verifyNoMoreInteractions(graphMock);

    }

}
