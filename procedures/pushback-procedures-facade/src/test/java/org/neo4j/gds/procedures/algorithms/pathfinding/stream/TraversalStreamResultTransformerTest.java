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
package org.neo4j.gds.procedures.algorithms.pathfinding.stream;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFactoryFacade;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.graphdb.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TraversalStreamResultTransformerTest {

    @Test
    void shouldTransformNonEmptyResult() {
        var graphMock = mock(Graph.class);
        var pathFactoryFacadeMock = mock(PathFactoryFacade.class);

        var transformer = new TraversalStreamResultTransformer(
            graphMock,
            pathFactoryFacadeMock,
            10
        );

        when(graphMock.toOriginalNodeId(anyLong()))
            // don't return the same original id as the input
            .thenAnswer(invocation -> ((long) invocation.getArgument(0)) + 19L);
        when(pathFactoryFacadeMock.createPath(any(long[].class), any(double[].class), any(), anyString()))
            .thenReturn(mock(Path.class));

        var streamResult = transformer.apply(new TimedAlgorithmResult<>(HugeLongArray.of(3,2,1,0), 1)).toList();

        assertThat(streamResult).hasSize(1);
        var result = streamResult.getFirst();
        assertThat(result.nodeIds()).containsExactly(22L, 21L, 20L, 19L);
        assertThat(result.sourceNode()).isEqualTo(10);
    }

}
