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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.graphdb.Path;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class BellmanFordStreamResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamOnEmptyResult() {
        var graphMock = mock(Graph.class);
        var closeableResourceRegistryMock = mock(CloseableResourceRegistry.class);
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);

        var transformer = new BellmanFordStreamResultTransformer(
            graphMock,
            closeableResourceRegistryMock,
            pathFactoryFacadeMock
        );

        var streamResult = transformer.apply(BellmanFordResult.EMPTY);
        assertThat(streamResult).isEmpty();

        verifyNoInteractions(graphMock);
        verifyNoInteractions(pathFactoryFacadeMock);
    }

    @Test
    void shouldTransformNonEmptyResult() {
        var graphMock = mock(Graph.class);
        var closeableResourceRegistryMock = mock(CloseableResourceRegistry.class);
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);

        var transformer = new BellmanFordStreamResultTransformer(
            graphMock,
            closeableResourceRegistryMock,
            pathFactoryFacadeMock
        );

        var bellmanFordResultMock = mock(BellmanFordResult.class);
        var pathResultMock = mock(PathResult.class);

        when(bellmanFordResultMock.containsNegativeCycle()).thenReturn(false);
        when(bellmanFordResultMock.shortestPaths()).thenReturn(new PathFindingResult(Stream.of(pathResultMock)));
        when(pathResultMock.nodeIds()).thenReturn(new long[]{1L, 2L});
        when(pathResultMock.costs()).thenReturn(new double[]{1.0});
        when(pathResultMock.index()).thenReturn(0L);
        when(pathResultMock.sourceNode()).thenReturn(1L);
        when(pathResultMock.targetNode()).thenReturn(2L);
        when(pathResultMock.totalCost()).thenReturn(1.0);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
        when(pathFactoryFacadeMock.createPath(any(long[].class), any(double[].class), any(), anyString()))
            .thenReturn(mock(Path.class));

        var streamResult = transformer.apply(bellmanFordResultMock).toList();

        assertThat(streamResult).hasSize(1);
        var result = streamResult.getFirst();
        assertThat(result.nodeIds()).containsExactly(1L, 2L);
        assertThat(result.costs()).containsExactly(1.0);
    }

    @Test
    void shouldTransformNegativeCycleResult() {
        var graphMock = mock(Graph.class);
        var closeableResourceRegistryMock = mock(CloseableResourceRegistry.class);
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);

        var transformer = new BellmanFordStreamResultTransformer(
            graphMock,
            closeableResourceRegistryMock,
            pathFactoryFacadeMock
        );

        var bellmanFordResultMock = mock(BellmanFordResult.class);
        var pathResultMock = mock(PathResult.class);

        when(bellmanFordResultMock.containsNegativeCycle()).thenReturn(true);
        when(bellmanFordResultMock.negativeCycles()).thenReturn(new PathFindingResult(Stream.of(pathResultMock)));
        when(pathResultMock.nodeIds()).thenReturn(new long[]{3L, 4L, 3L});
        when(pathResultMock.costs()).thenReturn(new double[]{-2.0, -1.0});
        when(pathResultMock.index()).thenReturn(1L);
        when(pathResultMock.sourceNode()).thenReturn(3L);
        when(pathResultMock.targetNode()).thenReturn(3L);
        when(pathResultMock.totalCost()).thenReturn(-3.0);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
        when(pathFactoryFacadeMock.createPath(any(long[].class), any(double[].class), any(), anyString()))
            .thenReturn(mock(Path.class));

        var streamResult = transformer.apply(bellmanFordResultMock).toList();

        assertThat(streamResult).hasSize(1);
        var result = streamResult.getFirst();
        assertThat(result.nodeIds()).containsExactly(3L, 4L, 3L);
        assertThat(result.costs()).containsExactly(-2.0, -1.0);
        assertThat(result.isNegativeCycle()).isTrue();
        assertThat(result.sourceNode()).isEqualTo(3L);
        assertThat(result.targetNode()).isEqualTo(3L);
        assertThat(result.totalCost()).isEqualTo(-3.0);
    }
}
