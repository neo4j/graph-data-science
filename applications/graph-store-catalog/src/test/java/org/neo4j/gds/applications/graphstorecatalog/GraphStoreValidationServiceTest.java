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
package org.neo4j.gds.applications.graphstorecatalog;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphStoreValidationServiceTest {
    @Test
    void shouldEnsureNodePropertiesExist() {
        var service = new GraphStoreValidationService();

        var graphStore = mock(GraphStore.class);
        when(graphStore.hasNodeProperty("foo")).thenReturn(true);
        when(graphStore.hasNodeProperty("bar")).thenReturn(true);
        service.ensureNodePropertiesExist(graphStore, List.of("foo", "bar"));

        // yep it didn't blow up :shrug:
    }

    @Test
    void shouldCatchNodePropertiesThatDoNotExist() {
        var service = new GraphStoreValidationService();

        var graphStore = mock(GraphStore.class);
        when(graphStore.hasNodeProperty("foo")).thenReturn(true);
        when(graphStore.nodePropertyKeys()).thenReturn(Set.of("foo"));
        assertThatIllegalArgumentException().isThrownBy(() -> {
            service.ensureNodePropertiesExist(graphStore, List.of("foo", "bar"));
        }).withMessage("Could not find property key(s) ['bar']. Defined keys: ['foo'].");
    }

    @Test
    void shouldEnsureRelationshipsDeletable() {
        var service = new GraphStoreValidationService();

        var graphStore = mock(GraphStore.class);

        when(graphStore.relationshipTypes()).thenReturn(Set.of(RelationshipType.of("foo"), RelationshipType.of("bar")));
        when(graphStore.hasNodeProperty("bar")).thenReturn(true);
        service.ensureRelationshipsMayBeDeleted(graphStore, "foo", GraphName.parse("some graph"));

        // yep it didn't blow up :shrug:
    }

    @Test
    void shouldDisallowDeletingLastRelationships() {
        var service = new GraphStoreValidationService();

        var graphStore = mock(GraphStore.class);
        when(graphStore.relationshipTypes()).thenReturn(Set.of(RelationshipType.of("foo")));
        assertThatIllegalArgumentException().isThrownBy(() -> service.ensureRelationshipsMayBeDeleted(
            graphStore,
            "foo",
            GraphName.parse("some graph")
        )).withMessage(
            "Deleting the last relationship type ('foo') from a graph ('some graph') is not supported. " +
                "Use `gds.graph.drop()` to drop the entire graph instead."
        );
    }

    @Test
    void shouldDisallowDeletingUnknownRelationships() {
        var service = new GraphStoreValidationService();

        var graphStore = mock(GraphStore.class);
        when(graphStore.relationshipTypes()).thenReturn(Set.of(RelationshipType.of("bar"), RelationshipType.of("baz")));
        assertThatIllegalArgumentException().isThrownBy(() -> service.ensureRelationshipsMayBeDeleted(
            graphStore,
            "foo",
            GraphName.parse("some graph")
        )).withMessage(
            "No relationship type 'foo' found in graph 'some graph'."
        );
    }
}
