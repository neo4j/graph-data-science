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
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.Aggregation;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;

class RelationshipSchemaTest {

    @Test
    void inAndOutUndirected() {
        assertTrue(RelationshipSchema.builder().build().isUndirected());

        assertTrue(RelationshipSchema
            .of(Map.of(
                RelationshipType.of("TYPE"),
                Map.of("prop", RelationshipPropertySchema.of("prop", ValueType.DOUBLE))
            ), Map.of(RelationshipType.of("TYPE"), UNDIRECTED))
            .isUndirected());
        assertFalse(RelationshipSchema.of(Map.of(
                RelationshipType.of("TYPE"),
                Map.of("prop", RelationshipPropertySchema.of("prop", ValueType.DOUBLE))
            ), Map.of(RelationshipType.of("TYPE"), NATURAL)).isUndirected()
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
                NATURAL,
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

    static Stream<Orientation> orientations() {
        return Stream.of(UNDIRECTED, NATURAL);
    }

    @ParameterizedTest
    @MethodSource("orientations")
    void testFiltering(Orientation orientation) {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema = RelationshipSchema.builder()
            .addProperty(label1, orientation, "bar", ValueType.DOUBLE)
            .addProperty(label1, orientation, "baz", ValueType.DOUBLE)
            .addProperty(label2, orientation, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(relationshipSchema, relationshipSchema.filter(Set.of(label1, label2)));

        var expected = RelationshipSchema.builder()
            .addProperty(label1, orientation, "bar", ValueType.DOUBLE)
            .addProperty(label1, orientation, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(expected, relationshipSchema.filter(Set.of(label1)));
        assertThat(relationshipSchema.isUndirected()).isEqualTo(orientation);
    }

    @Test
    void testFilteringMixed() {
        var directedType = RelationshipType.of("D");
        var undirectedType = RelationshipType.of("U");

        var directed = RelationshipSchema.builder()
            .addProperty(directedType, NATURAL, "bar", ValueType.DOUBLE)
            .build();
        var undirected = RelationshipSchema.builder()
            .addProperty(undirectedType, UNDIRECTED, "flob", ValueType.DOUBLE)
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
            Arguments.of(UNDIRECTED, UNDIRECTED, UNDIRECTED),
            Arguments.of(UNDIRECTED, NATURAL, NATURAL),
            Arguments.of(NATURAL, UNDIRECTED, NATURAL),
            Arguments.of(NATURAL, NATURAL, NATURAL)
        );
    }

    @ParameterizedTest
    @MethodSource("unionDirections")
    void testUnion(Orientation isUndirected1, Orientation isUndirected2, Orientation isUndirectedExpectation) {
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
        assertThat(actual.relTypeOrientationMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
            type1, isUndirected1,
            type2, isUndirected2
        ));
    }

    @Test
    void unionOnSameTypesFailsOnDirectionMismatch() {
        var schema1 = RelationshipSchema.builder()
            .addRelationshipType(RelationshipType.of("X"), NATURAL)
            .addRelationshipType(RelationshipType.of("Y"), UNDIRECTED)
            .build();

        var schema2 = RelationshipSchema.builder()
            .addRelationshipType(RelationshipType.of("X"), UNDIRECTED)
            .addRelationshipType(RelationshipType.of("Y"), UNDIRECTED)
            .build();

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Conflicting directionality for relationship types")
            .hasMessageContaining("X")
            .hasMessageNotContaining("Y");
    }



    @Test
    void unionOnSameTypeSamePropertyDifferentValueTypeFails() {
        var schema1 = RelationshipSchema.builder()
            .addProperty(RelationshipType.of("X"), NATURAL, "x", ValueType.DOUBLE)
            .addProperty(RelationshipType.of("Y"), UNDIRECTED, "unlikelypartofmessage", ValueType.DOUBLE)
            .build();

        var schema2 = RelationshipSchema.builder()
            .addProperty(RelationshipType.of("X"), NATURAL, "x", ValueType.LONG)
            .addProperty(RelationshipType.of("Y"), UNDIRECTED, "unlikelypartofmessage", ValueType.DOUBLE)
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
            Arguments.of(RelationshipSchema.builder().addRelationshipType(RelationshipType.of("A"), NATURAL).build(), false),
            Arguments.of(RelationshipSchema.builder().addRelationshipType(RelationshipType.of("A"), UNDIRECTED).build(), false),
            Arguments.of(RelationshipSchema.builder().addProperty(RelationshipType.of("A"), NATURAL, "foo", ValueType.LONG).build(), true),
            Arguments.of(RelationshipSchema.builder().addProperty(RelationshipType.of("A"), UNDIRECTED, "foo", ValueType.LONG).build(), true)
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

        builder.addRelationshipType(RelationshipType.of("X"), UNDIRECTED);
        builder.addRelationshipType(RelationshipType.of("Y"), NATURAL);

        RelationshipSchema schema = builder.build();
        assertFalse(schema.isUndirected());
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));
        assertThat(schema.properties()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), Map.of(),
            RelationshipType.of("Y"), Map.of()
        ));
        assertThat(schema.relTypeOrientationMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), UNDIRECTED,
            RelationshipType.of("Y"), NATURAL
        ));
    }

    @Test
    void testBuildProperties() {
        RelationshipSchema.Builder builder = RelationshipSchema.builder();

        builder.addProperty(RelationshipType.of("X"), UNDIRECTED, "x", ValueType.DOUBLE);
        builder.addProperty(RelationshipType.of("Y"), NATURAL, "y", ValueType.DOUBLE, Aggregation.MIN);
        builder.addProperty(RelationshipType.of("Z"), UNDIRECTED, "z", RelationshipPropertySchema.of("z", ValueType.DOUBLE));

        RelationshipSchema schema = builder.build();
        assertFalse(schema.isUndirected());
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));
        assertThat(schema.properties()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), Map.of("x", RelationshipPropertySchema.of("x", ValueType.DOUBLE)),
            RelationshipType.of("Y"), Map.of("y", RelationshipPropertySchema.of("y", ValueType.DOUBLE, Aggregation.MIN)),
            RelationshipType.of("Z"), Map.of("z", RelationshipPropertySchema.of("z", ValueType.DOUBLE))
        ));
        assertThat(schema.relTypeOrientationMap()).containsExactlyInAnyOrderEntriesOf(Map.of(
            RelationshipType.of("X"), UNDIRECTED,
            RelationshipType.of("Y"), NATURAL,
            RelationshipType.of("Z"), UNDIRECTED
        ));
    }
}
