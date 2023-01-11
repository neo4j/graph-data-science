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
package org.neo4j.gds.beta.indexInverse;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class InverseRelationshipsConfigTest {
    @GdlGraph(orientation = Orientation.NATURAL, indexInverse = true)
    private static final String DIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T1]->(b)" +
        ", (b)-[:T1]->(a)" +
        ", (b)-[:T2]->(c)" +
        ", (a)-[:T1]->(a)";

    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String UNDIRECTED = "()-[:X]->()";
    @Inject
    GraphStore graphStore;

    @Inject
    GraphStore undirectedGraphStore;

    @Test
    void supportString() {
        String input = "*";
        var config = InverseRelationshipsConfigImpl
            .builder()
            .relationshipTypes(input)
            .build();

        assertThat(config.relationshipTypes()).containsExactly(input);
    }

    @Test
    void supportList() {
        var input = List.of("T1", "T2");
        var config = InverseRelationshipsConfigImpl
            .builder()
            .relationshipTypes(input)
            .build();

        assertThat(config.relationshipTypes()).isEqualTo(input);
    }

    @Test
    void failIfAlreadyIndexed() {
        var config = InverseRelationshipsConfigImpl
            .builder()
            .relationshipTypes("T1")
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .hasMessage("Inverse index already exists for ['T1'].");
    }

    @Test
    void failOnUndirectedInput() {
        var config = InverseRelationshipsConfigImpl
            .builder()
            .relationshipTypes("X")
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            undirectedGraphStore,
            config.nodeLabelIdentifiers(undirectedGraphStore),
            config.internalRelationshipTypes(undirectedGraphStore)
        ))
            .hasMessage("Creating an inverse index for undirected relationships is not supported. Undirected relationship types are ['X'].");
    }
}
