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
package org.neo4j.gds.api.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.RelationshipPropertySchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.Aggregation;

import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipSchemaTest {

    @Test
    void testDefaultValuesAndAggregation() {
        var relType = RelationshipType.of("BAR");

        DefaultValue defaultValue = DefaultValue.of(42.0D);
        Aggregation aggregation = Aggregation.COUNT;
        GraphStore.PropertyState propertyState = GraphStore.PropertyState.PERSISTENT;
        String propertyName = "baz";
        var relationshipSchema = RelationshipSchema.builder()
            .addProperty(
                relType,
                propertyName,
                RelationshipPropertySchema.of(
                    propertyName,
                    ValueType.DOUBLE,
                    defaultValue,
                    propertyState,
                    aggregation
                )
            ).build();

        RelationshipPropertySchema relationshipPropertySchema = relationshipSchema.properties().get(relType).get(propertyName);
        assertTrue(relationshipPropertySchema.defaultValue().isUserDefined());

        assertEquals(defaultValue, relationshipPropertySchema.defaultValue());
        assertEquals(propertyState, relationshipPropertySchema.state());
        assertEquals(aggregation, relationshipPropertySchema.aggregation());
    }

    @Test
    void testFiltering() {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .addProperty(label2, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(relationshipSchema, relationshipSchema.filter(Set.of(label1, label2)));

        var expected = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(expected, relationshipSchema.filter(Set.of(label1)));
    }

    @Test
    void testUnion() {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema1 = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .build();

        var relationshipSchema2 = RelationshipSchema.builder()
            .addProperty(label2, "bar", ValueType.DOUBLE)
            .build();

        var expected = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label2, "bar", ValueType.DOUBLE)
            .build();

        assertEquals(expected, relationshipSchema1.union(relationshipSchema2));
    }

    @Test
    void testUnionProperties() {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema1 = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addRelationshipType(label2)
            .build();

        var relationshipSchema2 = RelationshipSchema.builder()
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .addProperty(label2, "baz", ValueType.DOUBLE)
            .build();

        var expected = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .addProperty(label2, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(expected, relationshipSchema1.union(relationshipSchema2));
    }

    @Test
    void testUnionOfIncompatibleProperties() {
        var label1 = RelationshipType.of("Foo");

        var relationshipSchema1 = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .build();

        var relationshipSchema2 = RelationshipSchema.builder()
            .addProperty(label1, "bar", ValueType.LONG)
            .build();


        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> relationshipSchema1.union(relationshipSchema2)
        );
        assertTrue(ex
            .getMessage()
            .contains("Combining schema entries with value type {bar=DOUBLE} and {bar=LONG} is not supported."));
    }


    static Stream<Arguments> schemaAndHasProperties() {
        return Stream.of(
            Arguments.of(RelationshipSchema.builder().addRelationshipType(RelationshipType.of("A")).build(), false),
            Arguments.of(RelationshipSchema.builder().addProperty(RelationshipType.of("A"), "foo", ValueType.LONG).build(), true)
        );
    }

    @ParameterizedTest
    @MethodSource("schemaAndHasProperties")
    void shouldKnowIfPropertiesArePresent(RelationshipSchema relationshipSchema, boolean hasProperties) {
        assertThat(relationshipSchema.hasProperties()).isEqualTo(hasProperties);
    }
}
