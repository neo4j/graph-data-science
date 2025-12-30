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
package org.neo4j.gds.centrality.validation;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.RelationshipSchema;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BetweennessCentralityGraphStoreValidationTest {

    @Test
    void shouldNotThrow(){
        Map<RelationshipType, Direction> directions = Map.of(
            RelationshipType.of("foo"),Direction.UNDIRECTED,
            RelationshipType.of("bar"),Direction.UNDIRECTED,
            RelationshipType.of("buzz"),Direction.DIRECTED
        );
        var relationshipSchema = mock(RelationshipSchema.class);
        when(relationshipSchema.directions()).thenReturn(directions);
        var schema = mock(GraphSchema.class);
        when(schema.relationshipSchema()).thenReturn(relationshipSchema);
        var graphStore = mock(GraphStore.class);
        when(graphStore.schema()).thenReturn(schema);

        var validation = new BetweennessCentralityGraphStoreValidation();

        assertThatNoException().isThrownBy(()->
            validation.validate(
            graphStore,
            List.of(),
            List.of(RelationshipType.of("foo"),RelationshipType.of("bar"))
        ));

    }


    @Test
    void shouldThrow(){
        Map<RelationshipType, Direction> directions = Map.of(
            RelationshipType.of("foo"),Direction.UNDIRECTED,
            RelationshipType.of("bar"),Direction.UNDIRECTED,
            RelationshipType.of("buzz"),Direction.DIRECTED
        );
        var relationshipSchema = mock(RelationshipSchema.class);
        when(relationshipSchema.directions()).thenReturn(directions);
        var schema = mock(GraphSchema.class);
        when(schema.relationshipSchema()).thenReturn(relationshipSchema);
        var graphStore = mock(GraphStore.class);
        when(graphStore.schema()).thenReturn(schema);

        var validation = new BetweennessCentralityGraphStoreValidation();
        assertThatThrownBy(()->{
            validation.validate(graphStore,List.of(), directions.keySet());
        }).hasMessageContaining("['bar (UNDIRECTED)', 'buzz (NATURAL)', 'foo (UNDIRECTED)']");
    }
}
