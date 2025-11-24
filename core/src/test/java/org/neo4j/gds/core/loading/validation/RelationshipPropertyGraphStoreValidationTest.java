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
package org.neo4j.gds.core.loading.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RelationshipPropertyGraphStoreValidationTest {


    @Test
    void shouldNotThrowIfPropertyIsEmpty() {
        var graphStore = mock(GraphStore.class);
        var validation = new RelationshipPropertyGraphStoreValidation(Optional.empty());
        var relType = new RelationshipType("FOO");
        when(graphStore.hasRelationshipProperty(eq(relType),eq("foo"))).thenThrow(new RuntimeException());

        assertThatNoException().isThrownBy(() -> validation.validate(
            graphStore,
            Set.of(),
            List.of(relType)
        ));
    }

    @Test
    void shouldNotThrowIfPropertyExists() {
        var graphStore = mock(GraphStore.class);
        var relType = new RelationshipType("FOO");
        when(graphStore.hasRelationshipProperty(eq(relType),eq("foo"))).thenReturn(true);
        var validation = new RelationshipPropertyGraphStoreValidation(Optional.of("test"));

        assertThatNoException().isThrownBy(() -> validation.validate(
            graphStore,
            Set.of(),
            List.of()
        ));
    }

    @Test
    void shouldThrowForMissingCustomProperty() {
        var graphStore = mock(GraphStore.class);
        var relType = new RelationshipType("FOO");
        var relTypes= List.of(relType);
        when(graphStore.hasRelationshipProperty(eq(relType),eq("foo"))).thenReturn(false);
        when(graphStore.relationshipPropertyKeys(eq(relTypes))).thenReturn(Set.of("buzz"));
        var validation = new RelationshipPropertyGraphStoreValidation(Optional.of("tests"),"bar");

        assertThatThrownBy(()-> validation.validate(
            graphStore,
            Set.of(),
            relTypes
        )).hasMessageContaining("Relationship property `tests` for parameter `bar` not found in relationship types ['FOO']. Properties existing on all relationship types: ['buzz']");
    }

    @Test
    void shouldThrowForMissingRelationshipWeightProperty() {
        var graphStore = mock(GraphStore.class);
        var relType = new RelationshipType("FOO");
        var relTypes= List.of(relType);
        when(graphStore.hasRelationshipProperty(eq(relType),eq("foo"))).thenReturn(false);
        when(graphStore.relationshipPropertyKeys(eq(relTypes))).thenReturn(Set.of("buzz"));

        var validation = new RelationshipPropertyGraphStoreValidation(Optional.of("test"));

        assertThatThrownBy(()-> validation.validate(
            graphStore,
            Set.of(),
            relTypes
        )).hasMessageContaining("Relationship weight property `test` not found in relationship types ['FOO']. Properties existing on all relationship types: ['buzz']");
    }


}
