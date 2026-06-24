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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodes.IdMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SourceNodeConfigTest {



    @Test
    void shouldThrowForInvalidNode() {
        var config = mock(SourceNodeConfig.class);
        when(config.sourceNode()).thenReturn(100L);

        var graphStore = mock(GraphStore.class);
        var idMap = mock(IdMap.class);
        when(graphStore.nodes()).thenReturn(idMap);
        when(idMap.safeToMappedNodeId(anyLong())).thenReturn(IdMap.NOT_FOUND);

        doCallRealMethod().when(config).validateSourceNode(graphStore, List.of(NodeLabel.of("Node")), List.of());
        assertThatThrownBy(() -> config.validateSourceNode(
            graphStore,
            List.of(NodeLabel.of("Node")),
            List.of()
        ))
            .hasMessageContaining("sourceNode nodes do not exist in the in-memory graph: [100]");
    }

    @Test
    void shouldNotThrowForExistingNode() {
        var config = mock(SourceNodeConfig.class);
        when(config.sourceNode()).thenReturn(100L);

        var graphStore = mock(GraphStore.class);
        var idMap = mock(IdMap.class);
        when(graphStore.nodes()).thenReturn(idMap);
        when(idMap.safeToMappedNodeId(anyLong())).thenReturn(0L);
        when(idMap.nodeLabels(anyLong())).thenReturn(List.of(NodeLabel.of("Node")));

        doCallRealMethod().when(config).validateSourceNode(graphStore, List.of(NodeLabel.of("Node")), List.of());
        assertThatNoException().isThrownBy(() -> config.validateSourceNode(
            graphStore,
            List.of(NodeLabel.of("Node")),
            List.of()
        ));
    }

}
