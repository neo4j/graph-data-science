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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class BfsConfigTest {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE " +
            "  (a:node)," +
            "  (b:node)," +
            "(a)-[:R]->(b)";


    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldNotAllowNegativeTargetNodes() {
        var config = BfsStreamConfigImpl.builder()
            .sourceNode(0L)
            .targetNodes(List.of(idFunction.of("a"), -1337));

        assertThatThrownBy(() -> config.build())
            .hasMessageContaining("Negative node ids are not supported for the field `targetNodes`");
    }

    @Test
    void failOnInvalidEndNodes() {
        var config = BfsStreamConfigImpl.builder()
            .sourceNode(idFunction.of("a"))
            .targetNodes(List.of(idFunction.of("b"), 421337)).build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        )).hasMessageContaining("targetNodes nodes do not exist in the in-memory graph: [421337]");
    }
}
