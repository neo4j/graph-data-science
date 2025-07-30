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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PrizeCollectingSteinerTreeResultTransformerTest {

    @Test
    void shouldReturnEmptyStreamOnEmptyResult() {
        var graphMock = mock(Graph.class);
        when(graphMock.nodeCount()).thenReturn(0L);

        var transformer = new PrizeCollectingSteinerTreeResultTransformer(
            graphMock
        );

        var streamResult = transformer.apply(PrizeSteinerTreeResult.EMPTY);
        assertThat(streamResult).isEmpty();
    }

    @Test
    void shouldTransformNonEmptyResult() {
        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenAnswer(invocation -> invocation.getArgument(0));
        when(graphMock.nodeCount()).thenReturn(3L);
        var transformer = new PrizeCollectingSteinerTreeResultTransformer(
            graphMock
        );

        var steinerResult = mock(PrizeSteinerTreeResult.class);
        when(steinerResult.parentArray()).thenReturn(HugeLongArray.of(4,3,1));
        when(steinerResult.relationshipToParentCost()).thenReturn(HugeDoubleArray.of(10,9,8));

        var streamResult = transformer.apply(steinerResult).toList();
        assertThat(streamResult.getFirst()).isEqualTo(new SpanningTreeStreamResult(0,4,10.0));
        assertThat(streamResult.get(1)).isEqualTo(new SpanningTreeStreamResult(1,3,9.0));
        assertThat(streamResult.getLast()).isEqualTo(new SpanningTreeStreamResult(2,1,8.0));

    }

}
