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
package org.neo4j.gds.similarity.filtering;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NodeIdNodeFilterSpecTest {

    @Test
    void shouldSucceedWhenAllNodesArePresent() {

        var nodeIdNodeFilterSpec = new NodeIdNodeFilterSpec(Set.of(1L, 2L, 3L, 5L, 8L, 42L));
        var graphStoreMock = mock(GraphStore.class);
        var nodesMock = mock(IdMap.class);
        doReturn(true).when(nodesMock).containsOriginalId(anyLong());
        when(graphStoreMock.nodes()).thenReturn(nodesMock);

        assertThatNoException().isThrownBy(() -> nodeIdNodeFilterSpec.validate(graphStoreMock, List.of(), "sourceNodeFilter"));
    }

    @Test
    void shouldFailOnMissingNodes() {

        var nodeIdNodeFilterSpec = new NodeIdNodeFilterSpec(Set.of(1L, 2L, 3L, 5L, 8L, 42L));
        var graphStoreMock = mock(GraphStore.class);
        var nodesMock = mock(IdMap.class);
        doReturn(true).when(nodesMock).containsOriginalId(anyLong());
        doReturn(false).when(nodesMock).containsOriginalId(3L);
        doReturn(false).when(nodesMock).containsOriginalId(42L);
        when(graphStoreMock.nodes()).thenReturn(nodesMock);

        assertThatThrownBy(
            () -> nodeIdNodeFilterSpec.validate(graphStoreMock, List.of(), "sourceNodeFilter")
        ).isInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith(
                "Invalid configuration value `sourceNodeFilter`, the following nodes are missing from the graph: [")
            .hasMessageContaining("3")
            .hasMessageContaining("42")
            .hasMessageEndingWith("]");
    }

}
