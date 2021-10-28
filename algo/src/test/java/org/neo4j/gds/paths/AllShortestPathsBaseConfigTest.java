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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfigImpl;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfigImpl;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AllShortestPathsBaseConfigTest {
    @Test
    void shouldAllowNodes() {
        var cypherMapWrapper = CypherMapWrapper
            .empty()
            .withEntry("sourceNode", new ShortestPathConfigTest.TestNode(42L));

        var config = new AllShortestPathsDijkstraStreamConfigImpl(Optional.of("graph"), Optional.empty(), cypherMapWrapper);

        assertThat(config.sourceNode()).isEqualTo(42L);
    }

    @Test
    void shouldAllowNodeIds() {
        var cypherMapWrapper = CypherMapWrapper
            .empty()
            .withEntry("sourceNode", 42L);

        var config = new AllShortestPathsDijkstraStreamConfigImpl(Optional.of("graph"), Optional.empty(), cypherMapWrapper);

        assertThat(config.sourceNode()).isEqualTo(42L);
    }

    @Test
    void shouldThrowErrorOnUnsupportedType() {
        var cypherMapWrapper = CypherMapWrapper
            .empty()
            .withEntry("sourceNode", "42");

        assertThatThrownBy(() -> new ShortestPathDijkstraStreamConfigImpl(Optional.of("graph"), Optional.empty(), cypherMapWrapper))
            .hasMessageContaining("Expected a node or a node id for `sourceNode`. Got String");
    }
}
