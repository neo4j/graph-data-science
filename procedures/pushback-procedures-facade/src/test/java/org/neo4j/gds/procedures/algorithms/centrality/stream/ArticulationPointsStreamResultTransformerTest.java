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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.articulationpoints.ArticulationPointsResult;
import org.neo4j.gds.articulationpoints.SubtreeTracker;
import org.neo4j.gds.procedures.algorithms.centrality.ArticulationPointStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ArticulationPointsStreamResultTransformerTest {

    @Test
    void shouldTransformResult(){

        var result = mock(ArticulationPointsResult.class);
        var bitset = new BitSet(3);
        bitset.set(1);

        when(result.subtreeTracker()).thenReturn(Optional.empty());
        when(result.articulationPoints()).thenReturn(bitset);

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(3L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new ArticulationPointsStreamResultTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new ArticulationPointStreamResult(1, null)
        );

        verify(graphMock, times(1)).toOriginalNodeId(1L);

    }

    @Test
    void shouldTransformResultAndReportStats(){

        var result = mock(ArticulationPointsResult.class);
        var bitset = new BitSet(3);
        bitset.set(1);
        var subtreeTracker = mock(SubtreeTracker.class);
        when(subtreeTracker.maxComponentSize(1)).thenReturn(10L);
        when(subtreeTracker.minComponentSize(1)).thenReturn(5L);
        when(subtreeTracker.remainingComponents(1)).thenReturn(8L);

        when(result.subtreeTracker()).thenReturn(Optional.of(subtreeTracker));
        when(result.articulationPoints()).thenReturn(bitset);

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(3L);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var transformer = new ArticulationPointsStreamResultTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(result, -1));
        assertThat(stream).containsExactlyInAnyOrder(
            new ArticulationPointStreamResult(1, Map.of("min",5L,"count",8L,"max",10L))
        );

        verify(graphMock, times(1)).toOriginalNodeId(1L);

    }

    @Test
    void shouldTransformEmptyResult(){

        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(0L);

        var transformer = new ArticulationPointsStreamResultTransformer(graphMock);

        var stream = transformer.apply(new TimedAlgorithmResult<>(ArticulationPointsResult.EMPTY, -1));
        assertThat(stream).isEmpty();

    }

}