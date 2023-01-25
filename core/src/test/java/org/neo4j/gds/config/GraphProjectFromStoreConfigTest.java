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
package org.neo4j.gds.config;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.core.Aggregation;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphProjectFromStoreConfigTest {

    @Test
    void testThrowOnOverlappingNodeProperties() {
        PropertyMappings propertyMappings = PropertyMappings.of(PropertyMapping.of(
            "duplicate",
            "foo",
            DefaultValue.of(0.0),
            Aggregation.NONE
        ));

        var nodeProjections = NodeProjections.single(
            NodeLabel.of("A"),
            NodeProjection.builder().label("A").properties(propertyMappings).build()
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ImmutableGraphProjectFromStoreConfig.builder()
                .graphName("graph")
                .relationshipProjections(ImmutableRelationshipProjections.of())
                .nodeProperties(propertyMappings)
                .nodeProjections(nodeProjections)
                .build()
        );

        assertThat(ex.getMessage(), allOf(containsString("node"), containsString("duplicate")));
    }

    @Test
    void testThrowOnOverlappingRelProperties() {
        var propertyMappings = PropertyMappings.of(PropertyMapping.of(
            "duplicate",
            "foo",
            DefaultValue.of(0.0),
            Aggregation.NONE
        ));

        RelationshipProjections relProjections = ImmutableRelationshipProjections.single(
            RelationshipType.of("A"),
            RelationshipProjection.builder()
                .type("A")
                .orientation(Orientation.NATURAL)
                .properties(propertyMappings)
                .build()
        );

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
            ImmutableGraphProjectFromStoreConfig.builder()
                .graphName("graph")
                .relationshipProperties(propertyMappings)
                .relationshipProjections(relProjections)
                .nodeProjections(NodeProjections.all())
                .build()
        );

        assertThat(ex.getMessage(), allOf(containsString("relationship"), containsString("duplicate")));
    }

    @Test
    void testMergingOfNodePropertiesAndProjections() {
        var propertyMappings1 = PropertyMappings.of(PropertyMapping.of(
                "foo",
                "foo",
                DefaultValue.of(0.0),
                Aggregation.NONE
            )
        );

        var propertyMappings2 = PropertyMappings.of(PropertyMapping.of(
            "bar",
            "foo",
            DefaultValue.of(0.0),
            Aggregation.NONE
        ));

        var nodeProjections = NodeProjections.single(
            NodeLabel.of("A"),
            NodeProjection.builder().label("A").properties(propertyMappings2).build()
        );

        GraphProjectFromStoreConfig graphProjectConfig = ImmutableGraphProjectFromStoreConfig.builder()
            .graphName("graph")
            .relationshipProjections(RelationshipProjections.ALL)
            .nodeProperties(propertyMappings1)
            .nodeProjections(nodeProjections)
            .build();

        Set<String> allProperties = graphProjectConfig.nodeProjections().allProperties();
        assertTrue(allProperties.contains("foo"));
        assertTrue(allProperties.contains("bar"));
        assertEquals(0, graphProjectConfig.nodeProperties().numberOfMappings());
    }

    @Test
    void testMergingOfRelationshipPropertiesAndProjections() {
        var propertyMappings1 = PropertyMappings.of(PropertyMapping.of("foo", "foo", DefaultValue.of(0.0), Aggregation.NONE));

        var propertyMappings2 = PropertyMappings.of(PropertyMapping.of(
            "bar",
            "foo",
            DefaultValue.of(0.0),
            Aggregation.NONE
        ));

        RelationshipProjections relProjections = ImmutableRelationshipProjections.single(
            RelationshipType.of("A"),
            RelationshipProjection.builder()
                .type("A")
                .orientation(Orientation.NATURAL)
                .properties(propertyMappings2)
                .build()
        );

        GraphProjectFromStoreConfig graphProjectConfig = ImmutableGraphProjectFromStoreConfig.builder()
            .graphName("graph")
            .nodeProjections(NodeProjections.all())
            .relationshipProperties(propertyMappings1)
            .relationshipProjections(relProjections)
            .build();

        Set<String> allProperties = graphProjectConfig.relationshipProjections().allProperties();
        assertTrue(allProperties.contains("foo"));
        assertTrue(allProperties.contains("bar"));
        assertEquals(0, graphProjectConfig.relationshipProperties().numberOfMappings());
    }
}
