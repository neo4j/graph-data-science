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
import org.neo4j.gds.api.schema.GraphSchema;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UndirectedOnlyRequirementTest {

    @Test
    void shouldNotThrowForUndirectedSchema() {
        var graphStore = mock(GraphStore.class);
        var schema = mock(GraphSchema.class);
        var schema2 = mock(GraphSchema.class);
        when(schema2.isUndirected()).thenReturn(true);
        when(graphStore.schema()).thenReturn(schema);
        when(schema.filterRelationshipTypes(anySet())).thenReturn(schema2);

        var validation = new UndirectedOnlyRequirement("foo");
        assertThatNoException().isThrownBy(() -> validation.validate(
            graphStore,
            null,
            List.of(RelationshipType.of("REL1"))
        ));

    }

    @Test
    void shouldThrowForDirectedSchema() {
        var graphStore = mock(GraphStore.class);
        var schema = mock(GraphSchema.class);
        var schema2 = mock(GraphSchema.class);
        when(schema2.isUndirected()).thenReturn(false);
        when(graphStore.schema()).thenReturn(schema);
        when(schema.filterRelationshipTypes(anySet())).thenReturn(schema2);


        var validation = new UndirectedOnlyRequirement("foo");
        assertThatThrownBy(() -> validation.validate(graphStore, null, List.of(RelationshipType.of("REL1"))))
            .hasMessageContaining("The foo algorithm requires relationship projections to be UNDIRECTED. " +
                "Selected relationships `[REL1]` are not all undirected");

    }

}
