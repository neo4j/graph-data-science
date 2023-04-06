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
package org.neo4j.gds.beta.undirected;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToUndirectedConfigTest {

    @Test
    void validateRelationshipType() {
        var builder = ToUndirectedConfigImpl.builder().relationshipType(" spaced_type ").mutateRelationshipType("X");

        assertThatThrownBy(builder::build)
            .hasMessage("`relationshipType` must not end or begin with whitespace characters, but got ` spaced_type `.");
    }

    @Test
    void failOnInvalidRelPropertiesInAggregation() {
        var graphStore = GdlFactory.of("()-[:R {p1: 2, p2: 3}]->()").build();

        var config = ToUndirectedConfigImpl.builder()
            .relationshipType("R")
            .mutateRelationshipType("X")
            .aggregation(Map.of("p1", "sum", "p3", "min", "p2", "max"))
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
                graphStore,
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        ).hasMessage("The `aggregation` parameter defines aggregations for ['p3'], which are not present on relationship type 'R'." +
                     " Available properties are: ['p1', 'p2'].");
    }

    @Test
    void failOnIncompletedRelPropertiesInAggregation() {
        var graphStore = GdlFactory.of("()-[:R {p1: 2, p2: 3}]->()").build();

        var config = ToUndirectedConfigImpl.builder()
            .relationshipType("R")
            .mutateRelationshipType("X")
            .aggregation(Map.of("p1", "sum"))
            .build();

        assertThatThrownBy(() -> config.graphStoreValidation(
                graphStore,
                config.nodeLabelIdentifiers(graphStore),
                config.internalRelationshipTypes(graphStore)
            )
        ).hasMessage("The `aggregation` parameter needs to define aggregations for each property on relationship type 'R'. Missing aggregations for ['p2'].");
    }

}
