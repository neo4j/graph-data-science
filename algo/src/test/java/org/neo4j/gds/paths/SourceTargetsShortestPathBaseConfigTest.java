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
package org.neo4j.gds.paths;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfigImpl;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SourceTargetsShortestPathBaseConfigTest {

    @Test
    void shouldWorkWithSingleTarget(){
        var config=ShortestPathDijkstraStreamConfigImpl.builder()
            .sourceNode(0L)
            .targetNode(2)
            .build();
        assertThat(config.targetsList()).containsExactly(2L);

    }

    @Test
    void shouldWorkWithSingleElementOnTargets() {
        var config = ShortestPathDijkstraStreamConfigImpl.builder()
            .sourceNode(0L)
            .targetNodes(2)
            .build();
        assertThat(config.targetsList()).containsExactly(2L);

    }

    @Test
    void shouldWorkWithManyTargets(){
        var config=ShortestPathDijkstraStreamConfigImpl.builder()
            .sourceNode(0L)
            .targetNodes(List.of(2,3))
            .build();
        assertThat(config.targetsList()).containsExactly(2L,3L);
    }

    @Test
    void shouldThrowIfBothSpecified(){
        assertThatThrownBy(() -> ShortestPathDijkstraStreamConfigImpl.builder().sourceNode(0).targetNode(2).targetNodes(List.of(3,3)).build()).hasMessageContaining("both");

    }

    @Test
    void shouldThrowIfNoneSpecified(){
        assertThatThrownBy(() -> ShortestPathDijkstraStreamConfigImpl.builder().sourceNode(0).build()).hasMessageContaining("One of");

    }

}
