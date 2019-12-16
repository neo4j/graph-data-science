/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.newapi;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.DeduplicationStrategy;

import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GraphCreateConfigTest {

    @Test
    void testThrowOnOverlappingNodeProperties() {
        final PropertyMappings propertyMappings = PropertyMappings.builder()
            .addMapping("duplicate", "foo", 0.0, DeduplicationStrategy.NONE)
            .build();

        final NodeProjections nodeProjections = NodeProjections.create(MapUtil.genericMap(
            new HashMap<>(),
            ElementIdentifier.of("A"), NodeProjection.of("A", propertyMappings)
        ));

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ImmutableGraphCreateConfig.builder()
                .graphName("graph")
                .relationshipProjection(RelationshipProjections.of())
                .nodeProperties(propertyMappings)
                .nodeProjection(nodeProjections)
                .build()
        );

        assertThat(ex.getMessage(), allOf(containsString("node"), containsString("duplicate")));
    }

    @Test
    void testThrowOnOverlappingRelProperties() {
        final PropertyMappings propertyMappings = PropertyMappings.builder()
            .addMapping("duplicate", "foo", 0.0, DeduplicationStrategy.NONE)
            .build();

        final RelationshipProjections relProjections = RelationshipProjections.single(
            ElementIdentifier.of("A"),
            RelationshipProjection.builder()
                .type("A")
                .projection(Projection.NATURAL)
                .properties(propertyMappings)
                .build()
        );

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ImmutableGraphCreateConfig.builder()
                .graphName("graph")
                .relationshipProperties(propertyMappings)
                .relationshipProjection(relProjections)
                .nodeProjection(NodeProjections.empty())
                .build()
        );

        assertThat(ex.getMessage(), allOf(containsString("relationship"), containsString("duplicate")));
    }

}