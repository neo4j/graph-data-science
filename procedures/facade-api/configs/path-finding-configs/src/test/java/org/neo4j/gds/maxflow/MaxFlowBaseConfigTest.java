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
package org.neo4j.gds.maxflow;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ListInputNodes;
import org.neo4j.gds.MapInputNodes;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MaxFlowBaseConfigTest {

    @Test
    void shouldThrowOnDuplicateNodeIds() {
        var configMock = mock(MaxFlowBaseConfig.class);
        when(configMock.sourceNodes()).thenReturn(new ListInputNodes(List.of(0L, 1L, 1L)));
        when(configMock.targetNodes()).thenReturn(new ListInputNodes(List.of(2L, 3L)));
        doCallRealMethod().when(configMock).assertNoDuplicateInputNodes();
        assertThatThrownBy(configMock::assertNoDuplicateInputNodes).hasMessageContaining("Source nodes must be unique.");

        when(configMock.sourceNodes()).thenReturn(new ListInputNodes(List.of(0L, 1L)));
        when(configMock.targetNodes()).thenReturn(new ListInputNodes(List.of(3L, 3L)));
        doCallRealMethod().when(configMock).assertNoDuplicateInputNodes();
        assertThatThrownBy(configMock::assertNoDuplicateInputNodes).hasMessageContaining("Target nodes must be unique.");

        when(configMock.sourceNodes()).thenReturn(new ListInputNodes(List.of(0L, 1L)));
        when(configMock.targetNodes()).thenReturn(new ListInputNodes(List.of(3L, 1L)));
        doCallRealMethod().when(configMock).assertNoDuplicateInputNodes();
        assertThatThrownBy(configMock::assertNoDuplicateInputNodes).hasMessageContaining(
            "Source and target nodes must be disjoint.");
    }

    @Test
    void shouldThrowOnNegativeValue() {
        var configMock = mock(MaxFlowBaseConfig.class);
        when(configMock.sourceNodes()).thenReturn(new MapInputNodes(Map.of(0L, 0.5D, 1L, -0.1D)));
        when(configMock.targetNodes()).thenReturn(new ListInputNodes(List.of(2L, 3L)));
        doCallRealMethod().when(configMock).assertNodeValuesArePositive();
        assertThatThrownBy(configMock::assertNodeValuesArePositive).hasMessageContaining(
            "Source node values must be positive, but found a negative value");

        when(configMock.sourceNodes()).thenReturn(new MapInputNodes(Map.of(0L, 0.5D, 1L, 0.1D)));
        when(configMock.targetNodes()).thenReturn(new MapInputNodes(Map.of(0L, 32.2D, 1L, -100D)));
        doCallRealMethod().when(configMock).assertNodeValuesArePositive();
        assertThatThrownBy(configMock::assertNodeValuesArePositive).hasMessageContaining(
            "Target node values must be positive, but found a negative value");
    }

    @Test
    void shouldThrowOnEmptyInputNodes() {
        var configMock = mock(MaxFlowBaseConfig.class);
        when(configMock.sourceNodes()).thenReturn(new MapInputNodes(Map.of()));
        when(configMock.targetNodes()).thenReturn(new ListInputNodes(List.of(2L, 3L)));
        doCallRealMethod().when(configMock).assertSourcesAndTargetsExist();
        assertThatThrownBy(configMock::assertSourcesAndTargetsExist).hasMessageContaining("Source nodes cannot be empty");

        when(configMock.sourceNodes()).thenReturn(new MapInputNodes(Map.of(2L, 0.5D, 0L, 10D)));
        when(configMock.targetNodes()).thenReturn(new ListInputNodes(List.of()));
        doCallRealMethod().when(configMock).assertSourcesAndTargetsExist();
        assertThatThrownBy(configMock::assertSourcesAndTargetsExist).hasMessageContaining("Target nodes cannot be empty");
    }
}
