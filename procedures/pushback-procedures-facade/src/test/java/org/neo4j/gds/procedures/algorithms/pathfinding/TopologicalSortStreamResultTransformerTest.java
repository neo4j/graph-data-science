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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class TopologicalSortStreamResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamOnEmptyResult() {
        var graphMock = mock(Graph.class);

        var transformer = new TopologicalSortStreamResultTransformer(
            graphMock
        );

        var streamResult = transformer.apply(TopologicalSortResult.EMPTY);
        assertThat(streamResult).isEmpty();

        verifyNoMoreInteractions(graphMock);
    }

    @Test
    void shouldTransformNonEmptyResultWithDistances() {
        var graphMock = mock(Graph.class);

        var transformer = new TopologicalSortStreamResultTransformer(
            graphMock
        );

        var topoSort = mock(TopologicalSortResult.class);
        var array = HugeAtomicDoubleArray.of(4, ParallelDoublePageCreator.of(new Concurrency(1), v -> v + 1.0));
        when(topoSort.maxSourceDistances()).thenReturn(Optional.of(array));
        when(topoSort.sortedNodes()).thenReturn(HugeLongArray.of(3, 0));
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var streamResult = transformer.apply(topoSort).toList();

        assertThat(streamResult).hasSize(2);
        assertThat(streamResult.getFirst()).isEqualTo(new TopologicalSortStreamResult(3, 4.0));
        assertThat(streamResult.getLast()).isEqualTo(new TopologicalSortStreamResult(0, 1.0));

    }

    @Test
    void shouldTransformNonEmptyResultWithoutDistances() {
        var graphMock = mock(Graph.class);

        var transformer = new TopologicalSortStreamResultTransformer(
            graphMock
        );

        var topoSort = mock(TopologicalSortResult.class);


        when(topoSort.sortedNodes()).thenReturn(HugeLongArray.of(3, 0));
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));

        var streamResult = transformer.apply(topoSort).toList();

        assertThat(streamResult).hasSize(2);
        assertThat(streamResult.getFirst()).isEqualTo(new TopologicalSortStreamResult(3, null));
        assertThat(streamResult.getLast()).isEqualTo(new TopologicalSortStreamResult(0, null));

    }


}
