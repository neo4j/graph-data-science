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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

class CSRGraphStoreTest {

    @Test
    void deleteAdditionalRelationshipTypes() {
        GdlFactory factory = GdlFactory.of("(b)-[:REL {x: 1}]->(a), (b)-[:REL]->(c)");
        var graphStore = factory.build();

        var del1 = graphStore.deleteRelationships(RelationshipType.of("REL"));

        assertThat(del1.deletedRelationships()).isEqualTo(2);
        assertThat(del1.deletedProperties()).containsEntry("x", 2L).hasSize(1);

        var del2 = graphStore.deleteRelationships(RelationshipType.of("REL"));

        assertThat(del2.deletedRelationships()).isEqualTo(0);
        assertThat(del2.deletedProperties()).isEmpty();
    }

    @Test
    void validateRelationshipTypesWhenNoneExist() {
        GdlFactory factory = GdlFactory.of("(a), (b)");
        var graphStore = factory.build();

        assertThatThrownBy(() -> graphStore.getGraph(
            List.of(NodeLabel.ALL_NODES),
            List.of(RelationshipType.of("X")),
            Optional.empty()
        )).hasMessageContaining("No relationships have been loaded for relationship type").hasMessageContaining("X");
    }

    @Test
    void gettingGraphsWithRelationshipTypes() {
        GdlFactory factory = GdlFactory.of("()-[:T]->()-[:R]->()-[:R]->()");
        var graphStore = factory.build();

        var t_graph = graphStore.getGraph(graphStore.nodeLabels(), List.of(RelationshipType.of("T")), Optional.empty());
        var r_graph = graphStore.getGraph(graphStore.nodeLabels(), List.of(RelationshipType.of("R")), Optional.empty());
        var t_r_graph = graphStore.getGraph(
            graphStore.nodeLabels(),
            List.of(RelationshipType.of("R"), RelationshipType.of("T")),
            Optional.empty()
        );
        var none_graph = graphStore.getGraph(graphStore.nodeLabels(), List.of(), Optional.empty());

        assertGraphEquals(fromGdl("()-[:T]->(), (), ()"), t_graph);
        assertGraphEquals(fromGdl("(), ()-[:R]->()-[:R]->()"), r_graph);
        assertGraphEquals(fromGdl("()-[:T]->()-[:R]->()-[:R]->()"), t_r_graph);
        assertGraphEquals(fromGdl("(), (), (), ()"), none_graph);
    }

}
