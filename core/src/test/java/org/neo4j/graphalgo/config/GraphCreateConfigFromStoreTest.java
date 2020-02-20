/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.config;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.core.Aggregation;

import java.util.Collections;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphCreateConfigFromStoreTest {

    @Test
    void testThrowOnOverlappingNodeProperties() {
        PropertyMappings propertyMappings = PropertyMappings.builder()
            .addMapping("duplicate", "foo", 0.0, Aggregation.NONE)
            .build();

        NodeProjections nodeProjections = NodeProjections.create(Collections.singletonMap(
            ElementIdentifier.of("A"), NodeProjection.of("A", propertyMappings)
        ));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ImmutableGraphCreateFromStoreConfig.builder()
                .graphName("graph")
                .relationshipProjections(RelationshipProjections.of())
                .nodeProperties(propertyMappings)
                .nodeProjections(nodeProjections)
                .build()
        );

        assertThat(ex.getMessage(), allOf(containsString("node"), containsString("duplicate")));
    }

    @Test
    void testThrowOnOverlappingRelProperties() {
        PropertyMappings propertyMappings = PropertyMappings.builder()
            .addMapping("duplicate", "foo", 0.0, Aggregation.NONE)
            .build();

        RelationshipProjections relProjections = RelationshipProjections.single(
            ElementIdentifier.of("A"),
            RelationshipProjection.builder()
                .type("A")
                .orientation(Orientation.NATURAL)
                .properties(propertyMappings)
                .build()
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ImmutableGraphCreateFromStoreConfig.builder()
                .graphName("graph")
                .relationshipProperties(propertyMappings)
                .relationshipProjections(relProjections)
                .nodeProjections(NodeProjections.empty())
                .build()
        );

        assertThat(ex.getMessage(), allOf(containsString("relationship"), containsString("duplicate")));
    }

    @Test
    void testMergingOfNodePropertiesAndProjections() {
        PropertyMappings propertyMappings1 = PropertyMappings.builder()
            .addMapping("foo", "foo", 0.0, Aggregation.NONE)
            .build();

        PropertyMappings propertyMappings2 = PropertyMappings.builder()
            .addMapping("bar", "foo", 0.0, Aggregation.NONE)
            .build();

        NodeProjections nodeProjections = NodeProjections.create(Collections.singletonMap(
            ElementIdentifier.of("A"), NodeProjection.of("A", propertyMappings2)
        ));

        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName("graph")
            .relationshipProjections(RelationshipProjections.of())
            .nodeProperties(propertyMappings1)
            .nodeProjections(nodeProjections)
            .build();

        Set<String> allProperties = graphCreateConfig.nodeProjections().allProperties();
        assertTrue(allProperties.contains("foo"));
        assertTrue(allProperties.contains("bar"));
        assertEquals(0, graphCreateConfig.nodeProperties().numberOfMappings());
    }

    @Test
    void testMergingOfRelationshipPropertiesAndProjections() {
        PropertyMappings propertyMappings1 = PropertyMappings.builder()
            .addMapping("foo", "foo", 0.0, Aggregation.NONE)
            .build();

        PropertyMappings propertyMappings2 = PropertyMappings.builder()
            .addMapping("bar", "foo", 0.0, Aggregation.NONE)
            .build();

        RelationshipProjections relProjections = RelationshipProjections.single(
            ElementIdentifier.of("A"),
            RelationshipProjection.builder()
                .type("A")
                .orientation(Orientation.NATURAL)
                .properties(propertyMappings2)
                .build()
        );

        GraphCreateConfig graphCreateConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName("graph")
            .nodeProjections(NodeProjections.empty())
            .relationshipProperties(propertyMappings1)
            .relationshipProjections(relProjections)
            .build();

        Set<String> allProperties = graphCreateConfig.relationshipProjections().allProperties();
        assertTrue(allProperties.contains("foo"));
        assertTrue(allProperties.contains("bar"));
        assertEquals(0, graphCreateConfig.relationshipProperties().numberOfMappings());
    }
}
