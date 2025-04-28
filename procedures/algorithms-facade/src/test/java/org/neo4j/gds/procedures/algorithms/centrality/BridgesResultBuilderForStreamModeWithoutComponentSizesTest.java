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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.bridges.Bridge;
import org.neo4j.gds.bridges.BridgeResult;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BridgesResultBuilderForStreamModeWithoutComponentSizesTest {

    @Test
    void shouldMapCorrectly() {
        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenReturn(0L, 1L);

        var bridgeResult = new BridgeResult(
            List.of(
                new Bridge(0, 1, new long[] {5, 19})
            )
        );

        var bridgesResultStream = new BridgesResultBuilderForStreamModeWithoutComponentSizes().build(
            graphMock,
            mock(GraphStore.class),
            Optional.of(bridgeResult)
        );

        assertThatNoException()
            .isThrownBy(() -> assertThat(bridgesResultStream)
                .containsExactly(new BridgesStreamResult(0, 1, null)));
    }

    @Test
    void shouldNotThrowExceptionWhenRemainingSizesIsNull() {
        var graphMock = mock(Graph.class);
        when(graphMock.toOriginalNodeId(anyLong())).thenReturn(0L, 1L);

        var bridgeResult = new BridgeResult(
            List.of(
                new Bridge(0, 1, null)
            )
        );

        var bridgesResultStream = new BridgesResultBuilderForStreamModeWithoutComponentSizes().build(
            graphMock,
            mock(GraphStore.class),
            Optional.of(bridgeResult)
        );

        assertThatNoException()
            // need to consume the stream to trigger processing the results
            .isThrownBy(() -> assertThat(bridgesResultStream)
                .containsExactly(new BridgesStreamResult(0, 1, null)));
    }

    @Test
    void shouldReturnEmptyStreamWhenTheAlgorithmResultIsEmpty() {
        var bridgesResultStream = new BridgesResultBuilderForStreamModeWithoutComponentSizes().build(
            mock(Graph.class),
            mock(GraphStore.class),
            Optional.empty()
        );

        assertThat(bridgesResultStream).isEmpty();
    }

}
