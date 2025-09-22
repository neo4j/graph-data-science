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

package org.neo4j.gds.procedures.algorithms.embeddings.stats;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.procedures.algorithms.embeddings.FastRPStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class FastRPStatsResultTransformerTest {

    @Test
    void shouldTransformFastRPResultToStatsResultStream() {
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(42L);

        var fastRPResultMock = mock(FastRPResult.class);
        var config = Map.<String, Object>of("dim", 128, "seed", 42);

        var timedResult = new TimedAlgorithmResult<>(fastRPResultMock, 1234);

        var transformer = new FastRPStatsResultTransformer(graphMock, config);

        var resultStream = transformer.apply(timedResult);

        assertThat(resultStream)
            .hasSize(1)
            .first()
            .isEqualTo(new FastRPStatsResult(42L, -1, 1234L, config));

        verify(graphMock, times(1)).nodeCount();
        verifyNoMoreInteractions(graphMock);
    }

    @Test
    void shouldReturnStatsResultWithZeroNodesWhenTimedAlgorithmResultIsEmpty() {
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(0L);

        var config = Map.<String, Object>of("dim", 128, "seed", 42);

        var timedResult = TimedAlgorithmResult.empty(FastRPResult.empty());

        var transformer = new FastRPStatsResultTransformer(graphMock, config);

        var resultStream = transformer.apply(timedResult);

        assertThat(resultStream)
            .hasSize(1)
            .first()
            .isEqualTo(new FastRPStatsResult(0L, -1, 0L, config));

        verify(graphMock, times(1)).nodeCount();
        verifyNoMoreInteractions(graphMock);
    }
}
