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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.procedures.algorithms.embeddings.DefaultNodeEmbeddingsStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class FastRPStreamResultTransformerTest {

    @Test
    void shouldTransformFastRPResultToStream() {
        var graphMock = mock(Graph.class);
        var embeddings = HugeObjectArray.newArray(float[].class, 2);
        embeddings.set(0, new float[]{1.0f, 2.0f});
        embeddings.set(1, new float[]{3.0f, 4.0f});
        var timedResult = new TimedAlgorithmResult<>(new FastRPResult(embeddings), 0);

        when(graphMock.toOriginalNodeId(anyLong())).thenReturn(19L, 42L);

        var transformer = new FastRPStreamResultTransformer(graphMock);

        var resultStream = transformer.apply(timedResult);

        assertThat(resultStream)
            .hasSize(2)
            .containsExactly(
                new DefaultNodeEmbeddingsStreamResult(19L, List.of(1.0d, 2.0d)),
                new DefaultNodeEmbeddingsStreamResult(42L, List.of(3.0d, 4.0d))
            );

        verify(graphMock, times(1)).toOriginalNodeId(0L);
        verify(graphMock, times(1)).toOriginalNodeId(1L);
        verifyNoMoreInteractions(graphMock);
    }

    @Test
    void shouldReturnEmptyStreamWhenTimedAlgorithmResultIsEmpty() {
        var graphMock = mock(Graph.class);
        var timedResult = TimedAlgorithmResult.empty(FastRPResult.empty());

        var transformer = new FastRPStreamResultTransformer(graphMock);

        var resultStream = transformer.apply(timedResult);
        assertThat(resultStream).isEmpty();

        verifyNoInteractions(graphMock);
    }
}
