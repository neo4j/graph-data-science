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

class PathFindingStreamResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamOnEmptyResult() {
        var graphMock = mock(Graph.class);
        var closeableResourceRegistryMock = mock(CloseableResourceRegistry.class);
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);

        var transformer = new PathFindingStreamResultTransformer(
            graphMock,
            closeableResourceRegistryMock,
            pathFactoryFacadeMock
        );

        var streamResult = transformer.apply(PathFindingResult.empty());
        assertThat(streamResult).isEmpty();

        verifyNoInteractions(graphMock);
        verifyNoInteractions(pathFactoryFacadeMock);
    }

    @Test
    void shouldTransformNonEmptyResult() {
        var graphMock = mock(Graph.class);
        var closeableResourceRegistryMock = mock(CloseableResourceRegistry.class);
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);

        var transformer = new PathFindingStreamResultTransformer(
            graphMock,
            closeableResourceRegistryMock,
            pathFactoryFacadeMock
        );

        var pathResultMock = mock(PathResult.class);

        when(pathResultMock.nodeIds()).thenReturn(new long[]{1L, 2L});
        when(pathResultMock.costs()).thenReturn(new double[]{1.0});
        when(pathResultMock.index()).thenReturn(0L);
        when(pathResultMock.sourceNode()).thenReturn(1L);
        when(pathResultMock.targetNode()).thenReturn(2L);
        when(pathResultMock.totalCost()).thenReturn(1.0);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
        when(pathFactoryFacadeMock.createPath(any(long[].class), any(double[].class), any(), anyString()))
            .thenReturn(mock(Path.class));

        var result =new PathFindingResult(Stream.of(pathResultMock));

        var streamResult = transformer.apply(result).toList();

        assertThat(streamResult).hasSize(1);
        var pathResult = streamResult.getFirst();
        assertThat(pathResult.nodeIds()).containsExactly(1L, 2L);
        assertThat(pathResult.costs()).containsExactly(1.0);
    }

}
