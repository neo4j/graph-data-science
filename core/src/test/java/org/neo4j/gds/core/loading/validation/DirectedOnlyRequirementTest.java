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
import org.neo4j.gds.api.schema.RelationshipSchema;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DirectedOnlyRequirementTest {

    @Test
    void shouldNotThrowForDirectedSchema() {
        var relType1 = RelationshipType.of("REL1");
        var relType2 = RelationshipType.of("REL2");
        var relSchema = mock(RelationshipSchema.class);
        when(relSchema.isUndirected(eq(relType1))).thenReturn(false);
        when(relSchema.isUndirected(eq(relType2))).thenReturn(false);
        var graphStore = mock(GraphStore.class);
        var schema = mock(GraphSchema.class);
        when(schema.relationshipSchema()).thenReturn(relSchema);
        when(graphStore.schema()).thenReturn(schema);

        var validation = new DirectedOnlyRequirement("foo");
        assertThatNoException().isThrownBy(() -> validation.validate(
            graphStore,
            null,
            List.of(relType1,relType2)
        ));

    }

    @Test
    void shouldThrowForUndirectedSchema() {

        var relType1 = RelationshipType.of("REL1");
        var relType2 = RelationshipType.of("REL2");
        var relSchema = mock(RelationshipSchema.class);
        when(relSchema.isUndirected(eq(relType1))).thenReturn(true);
        when(relSchema.isUndirected(eq(relType2))).thenReturn(false);
        var graphStore = mock(GraphStore.class);
        var schema = mock(GraphSchema.class);
        when(schema.relationshipSchema()).thenReturn(relSchema);
        when(graphStore.schema()).thenReturn(schema);

        var validation = new DirectedOnlyRequirement("foo");
        assertThatThrownBy(() -> validation.validate(graphStore, null, List.of(relType2,relType1)))
            .hasMessageContaining("The foo algorithm requires relationship projections to be directed. " +
                "Selected relationships `[REL1]` are not all directed");

    }

}
