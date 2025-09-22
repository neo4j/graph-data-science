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

package org.neo4j.gds.procedures.algorithms.embeddings.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingsStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class HashGNNStreamResultTransformerTest {

    @Test
    void shouldTransformHashGNNResultToStream() {
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(2L);
        when(graphMock.toOriginalNodeId(anyLong())).thenReturn(10L,20L);
        var nodePropertyValuesMock = mock(NodePropertyValues.class);

        when(nodePropertyValuesMock.doubleArrayValue(0L)).thenReturn(new double[] {1.0, 2.0});
        when(nodePropertyValuesMock.doubleArrayValue(1L)).thenReturn(new double[] {3.0, 4.0});
        var hashGNNResult = new HashGNNResult(nodePropertyValuesMock);

        var timedResult = new TimedAlgorithmResult<>(hashGNNResult, 0);

        var transformer = new HashGNNStreamResultTransformer(graphMock);

        var resultStream = transformer.apply(timedResult);

        assertThat(resultStream)
            .hasSize(2)
            .containsExactly(
                new DefaultNodeEmbeddingsStreamResult(10L, List.of(1.0, 2.0)),
                new DefaultNodeEmbeddingsStreamResult(20L, List.of(3.0, 4.0))
            );

        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verify(graphMock, times(1)).nodeCount();
        verify(nodePropertyValuesMock, times(1)).doubleArrayValue(0L);
        verify(nodePropertyValuesMock, times(1)).doubleArrayValue(1L);
        verifyNoMoreInteractions(graphMock, nodePropertyValuesMock);
    }

    @Test
    void shouldReturnEmptyStreamWhenTimedAlgorithmResultIsEmpty() {
        var graphMock = mock(Graph.class);

        when(graphMock.nodeCount()).thenReturn(0L);

        var timedResult = TimedAlgorithmResult.empty(HashGNNResult.empty());

        var transformer = new HashGNNStreamResultTransformer(graphMock);

        var resultStream = transformer.apply(timedResult);

        assertThat(resultStream).isEmpty();

        verify(graphMock, times(1)).nodeCount();
        verifyNoMoreInteractions(graphMock);
    }
}
