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
package org.neo4j.gds.hits;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HitsCompanionTest {

    @Test
    void shouldCollectRelationshipsWithoutType(){
        var graphStore = mock(GraphStore.class);
        List<RelationshipType> relTypes  = List.of(RelationshipType.of("R1"), RelationshipType.of("R2") );
        when(graphStore.inverseIndexedRelationshipTypes()).thenReturn(Set.of( RelationshipType.of("R2"),  RelationshipType.of("R3")));

        var relTypesWithoutIndex = HitsCompanion.relationshipsWithoutIndices(
            graphStore,
            relTypes
        );

        assertThat(relTypesWithoutIndex).containsExactly(RelationshipType.of("R1"));

    }

}
