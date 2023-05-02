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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.TargetNodeConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class TargetNodeConfigTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
            " (a0:Node)," +
            " (a1:Node)";

    @Inject
    GraphStore graphStore;
    @Inject
    IdFunction idFunction;

    @Test
    void shouldThrowForInvalidNode() {
        var config = SampleTargetConfigImpl.builder().targetNode(100).build();

        assertThatThrownBy(() -> config.validateTargetNode(
            graphStore,
            List.of(NodeLabel.of("Node")),
            List.of()
        ))
            .hasMessageContaining("Target node does not exist in the in-memory graph: `100`");
    }

    @Test
    void shouldNotThrowForExistingNode() {
        var config = SampleTargetConfigImpl.builder().targetNode(idFunction.of("a0")).build();

        assertThatNoException().isThrownBy(() -> config.validateTargetNode(
            graphStore,
            List.of(NodeLabel.of("Node")),
            List.of()
        ));
    }

    @Configuration
    interface SampleTargetConfig extends TargetNodeConfig {

    }
}
