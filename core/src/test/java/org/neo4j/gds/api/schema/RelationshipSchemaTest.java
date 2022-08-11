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
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.Aggregation;
import scala.Enumeration;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationshipSchemaTest {

    @Test
    void inAndOutUndirected() {
        assertTrue(RelationshipSchema.builder().build().isUndirected());

        assertTrue(RelationshipSchema
            .of(Map.of(
                RelationshipType.of("TYPE"),
                Map.of("prop", RelationshipPropertySchema.of("prop", ValueType.DOUBLE))
            ), Map.of(RelationshipType.of("TYPE"), true))
            .isUndirected());
        assertFalse(RelationshipSchema.of(Map.of(
                RelationshipType.of("TYPE"),
                Map.of("prop", RelationshipPropertySchema.of("prop", ValueType.DOUBLE))
            ), Map.of(RelationshipType.of("TYPE"), false)).isUndirected()
        );
    }

    @Test
    void emptyIsUndirected() {
        assertTrue(RelationshipSchema.empty().isUndirected());
    }

    @Test
    void handlesOutsideOfSchemaRequests() {
        var empty = RelationshipSchema.empty();
        assertFalse(empty.hasProperty(RelationshipType.of("NotInSchema"), "notInSchemaEither"));
    }

    @Test
    void testDefaultValuesAndAggregation() {
        var relType = RelationshipType.of("BAR");

        DefaultValue defaultValue = DefaultValue.of(42.0D);
        Aggregation aggregation = Aggregation.COUNT;
        PropertyState propertyState = PropertyState.PERSISTENT;
        String propertyName = "baz";
        var relationshipSchema = RelationshipSchema.builder()
            .addProperty(
                relType,
                false,
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testFiltering(boolean isUndirected) {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema = RelationshipSchema.builder()
            .addProperty(label1, isUndirected, "bar", ValueType.DOUBLE)
            .addProperty(label1, isUndirected, "baz", ValueType.DOUBLE)
            .addProperty(label2, isUndirected, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(relationshipSchema, relationshipSchema.filter(Set.of(label1, label2)));

        var expected = RelationshipSchema.builder()
            .addProperty(label1, isUndirected, "bar", ValueType.DOUBLE)
            .addProperty(label1, isUndirected, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(expected, relationshipSchema.filter(Set.of(label1)));
        assertThat(relationshipSchema.isUndirected()).isEqualTo(isUndirected);
    }

    @Test
    void testFilteringMixed() {
        var directedType = RelationshipType.of("D");
        var undirectedType = RelationshipType.of("U");

        var directed = RelationshipSchema.builder()
            .addProperty(directedType, false, "bar", ValueType.DOUBLE)
            .build();
        var undirected = RelationshipSchema.builder()
            .addProperty(undirectedType, true, "flob", ValueType.DOUBLE)
            .build();
        var mixed = directed.union(undirected);

        assertFalse(mixed.isUndirected());
        assertFalse(directed.isUndirected());
        assertTrue(undirected.isUndirected());
        assertEquals(mixed, mixed.filter(Set.of(directedType, undirectedType)));
        assertEquals(directed, mixed.filter(Set.of(directedType)));
        assertEquals(undirected, mixed.filter(Set.of(undirectedType)));
    }

    static Stream<Arguments> unionDirections() {
        return Stream.of(
            Arguments.of(true, true, true),
            Arguments.of(true, false, false),
            Arguments.of(false, true, false),
            Arguments.of(false, false, false)
        );
    }

    @ParameterizedTest
    @MethodSource("unionDirections")
    void testUnion(boolean isUndirected1, boolean isUndirected2, boolean isUndirectedExpectation) {
        var type1 = RelationshipType.of("Foo");
        var type2 = RelationshipType.of("Bar");

        var relationshipSchema1 = RelationshipSchema.builder()
            .addProperty(type1, isUndirected1, "bar", ValueType.DOUBLE)
            .build();

        var relationshipSchema2 = RelationshipSchema.builder()
            .addProperty(type2, isUndirected2, "bar", ValueType.DOUBLE)
            .build();

        var expected = RelationshipSchema.builder()
            .addProperty(type1, isUndirected1, "bar", ValueType.DOUBLE)
            .addProperty(type2, isUndirected2, "bar", ValueType.DOUBLE)
            .build();

        var actual = relationshipSchema1.union(relationshipSchema2);
        assertEquals(expected, actual);
        assertThat(actual.isUndirected()).isEqualTo(isUndirectedExpectation);
        assertThat(actual.typeIsUndirected()).containsExactlyInAnyOrderEntriesOf(Map.of(
            type1, isUndirected1,
            type2, isUndirected2
        ));
    }

    @Test
    void unionOnSameTypesFailsOnDirectionMismatch() {
        var schema1 = RelationshipSchema.builder()
            .addRelationshipType(RelationshipType.of("X"), false)
            .addRelationshipType(RelationshipType.of("Y"), true)
            .build();

        var schema2 = RelationshipSchema.builder()
            .addRelationshipType(RelationshipType.of("X"), true)
            .addRelationshipType(RelationshipType.of("Y"), true)
            .build();

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Conflicting directionality for relationship types")
            .hasMessageContaining("X")
            .hasMessageNotContaining("Y");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void unionOnSameTypeSamePropertyDifferentValueTypeFails(boolean isUndirected) {
        var schema1 = RelationshipSchema.builder()
            .addProperty(RelationshipType.of("X"), false, "x", ValueType.DOUBLE)
            .addProperty(RelationshipType.of("Y"), true, "unlikelypartofmessage", ValueType.DOUBLE)
            .build();

        var schema2 = RelationshipSchema.builder()
            .addProperty(RelationshipType.of("X"), false, "x", ValueType.LONG)
            .addProperty(RelationshipType.of("Y"), true, "unlikelypartofmessage", ValueType.DOUBLE)
            .build();

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Combining schema entries with value type")
            .hasMessageContaining("x")
            .hasMessageContaining("DOUBLE")
            .hasMessageContaining("LONG")
            .hasMessageNotContaining("unlikelypartofmessage");
    }

    static Stream<Arguments> schemaAndHasProperties() {
        return Stream.of(
            Arguments.of(RelationshipSchema.builder().addRelationshipType(RelationshipType.of("A"), false).build(), false),
            Arguments.of(RelationshipSchema.builder().addRelationshipType(RelationshipType.of("A"), true).build(), false),
            Arguments.of(RelationshipSchema.builder().addProperty(RelationshipType.of("A"), false, "foo", ValueType.LONG).build(), true),
            Arguments.of(RelationshipSchema.builder().addProperty(RelationshipType.of("A"), true, "foo", ValueType.LONG).build(), true)
        );
    }

    @ParameterizedTest
    @MethodSource("schemaAndHasProperties")
    void shouldKnowIfPropertiesArePresent(RelationshipSchema relationshipSchema, boolean hasProperties) {
        assertThat(relationshipSchema.hasProperties()).isEqualTo(hasProperties);
    }

    @Test
    void testBuildRelTypes() {
        RelationshipSchema.Builder builder = RelationshipSchema.builder();

        builder.addRelationshipType(RelationshipType.of("X"), true);
        builder.addRelationshipType(RelationshipType.of("Y"), false);

        RelationshipSchema schema = builder.build();
        assertFalse(schema.isUndirected());
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));
        assertThat(schema.properties()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), Map.of(),
            RelationshipType.of("Y"), Map.of()
        ));
        assertThat(schema.typeIsUndirected()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), true,
            RelationshipType.of("Y"), false
        ));
    }

    @Test
    void testBuildProperties() {
        RelationshipSchema.Builder builder = RelationshipSchema.builder();

        builder.addProperty(RelationshipType.of("X"), true, "x", ValueType.DOUBLE);
        builder.addProperty(RelationshipType.of("Y"), false, "y", ValueType.DOUBLE, Aggregation.MIN);
        builder.addProperty(RelationshipType.of("Z"), true, "z", RelationshipPropertySchema.of("z", ValueType.DOUBLE));

        RelationshipSchema schema = builder.build();
        assertFalse(schema.isUndirected());
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));
        assertThat(schema.properties()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), Map.of("x", RelationshipPropertySchema.of("x", ValueType.DOUBLE)),
            RelationshipType.of("Y"), Map.of("y", RelationshipPropertySchema.of("y", ValueType.DOUBLE, Aggregation.MIN)),
            RelationshipType.of("Z"), Map.of("z", RelationshipPropertySchema.of("z", ValueType.DOUBLE))
        ));
        assertThat(schema.typeIsUndirected()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), true,
            RelationshipType.of("Y"), false,
            RelationshipType.of("Z"), true
        ));
    }
}
