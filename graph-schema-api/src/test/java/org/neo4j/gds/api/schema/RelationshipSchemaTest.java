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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.Orientation.UNDIRECTED;

class RelationshipSchemaTest {

    @Test
    void inAndOutUndirected() {
        assertThat(RelationshipSchema.empty().isUndirected()).isTrue();

        RelationshipType type = RelationshipType.of("TYPE");
        var propertySchema = Map.of(
            type,
            Map.of("prop", RelationshipPropertySchema.of("prop", ValueType.DOUBLE))
        );

        var undirectedSchema = RelationshipSchema.empty();
        undirectedSchema.getOrCreateRelationshipType(type, UNDIRECTED);
        assertThat(undirectedSchema.isUndirected()).isTrue();

        var directedSchema = RelationshipSchema.empty();
        directedSchema.getOrCreateRelationshipType(type, NATURAL);
        assertThat(directedSchema.isUndirected()).isFalse();
    }

    @Test
    void emptyIsUndirected() {
        assertThat(RelationshipSchema.empty().isUndirected()).isTrue();
    }

    @Test
    void handlesOutsideOfSchemaRequests() {
        var empty = RelationshipSchema.empty();
        assertThat(empty.hasProperty(RelationshipType.of("NotInSchema"), "notInSchemaEither")).isFalse();
    }

    @Test
    void testDefaultValuesAndAggregation() {
        var relType = RelationshipType.of("BAR");

        DefaultValue defaultValue = DefaultValue.of(42.0D);
        Aggregation aggregation = Aggregation.COUNT;
        PropertyState propertyState = PropertyState.PERSISTENT;
        String propertyName = "baz";
        var relationshipSchema = RelationshipSchema.empty()
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
            );

        RelationshipPropertySchema relationshipPropertySchema = relationshipSchema
            .get(relType)
            .properties()
            .get(propertyName);
        assertThat(relationshipPropertySchema.defaultValue().isUserDefined()).isTrue();

        assertThat(relationshipPropertySchema.defaultValue()).isEqualTo(defaultValue);
        assertThat(relationshipPropertySchema.state()).isEqualTo(propertyState);
        assertThat(relationshipPropertySchema.aggregation()).isEqualTo(aggregation);
    }

    static Stream<Arguments> orientations() {
        return Stream.of(
            Arguments.of(UNDIRECTED, true),
            Arguments.of(NATURAL, false)
        );
    }

    @ParameterizedTest
    @MethodSource("orientations")
    void testFiltering(Orientation orientation, boolean isUndirected) {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema = RelationshipSchema.empty()
            .addProperty(label1, orientation, "bar", ValueType.DOUBLE)
            .addProperty(label1, orientation, "baz", ValueType.DOUBLE)
            .addProperty(label2, orientation, "baz", ValueType.DOUBLE);

        assertThat(relationshipSchema.filter(Set.of(label1, label2))).isEqualTo(relationshipSchema);

        var expected = RelationshipSchema.empty()
            .addProperty(label1, orientation, "bar", ValueType.DOUBLE)
            .addProperty(label1, orientation, "baz", ValueType.DOUBLE);

        assertThat(relationshipSchema.filter(Set.of(label1))).isEqualTo(expected);
        assertThat(relationshipSchema.isUndirected()).isEqualTo(isUndirected);
    }

    @Test
    void testFilteringMixed() {
        var directedType = RelationshipType.of("D");
        var undirectedType = RelationshipType.of("U");

        var directed = RelationshipSchema.empty()
            .addProperty(directedType, NATURAL, "bar", ValueType.DOUBLE);
        var undirected = RelationshipSchema.empty()
            .addProperty(undirectedType, UNDIRECTED, "flob", ValueType.DOUBLE);
        var mixed = directed.union(undirected);

        assertThat(mixed.isUndirected()).isFalse();
        assertThat(directed.isUndirected()).isFalse();
        assertThat(undirected.isUndirected()).isTrue();
        assertThat(mixed.filter(Set.of(directedType, undirectedType))).isEqualTo(mixed);
        assertThat(mixed.filter(Set.of(directedType))).isEqualTo(directed);
        assertThat(mixed.filter(Set.of(undirectedType))).isEqualTo(undirected);
    }

    static Stream<Arguments> unionDirections() {
        return Stream.of(
            Arguments.of(UNDIRECTED, UNDIRECTED, true),
            Arguments.of(UNDIRECTED, NATURAL, false),
            Arguments.of(NATURAL, UNDIRECTED, false),
            Arguments.of(NATURAL, NATURAL, false)
        );
    }

    @ParameterizedTest
    @MethodSource("unionDirections")
    void testUnion(Orientation isUndirected1, Orientation isUndirected2, Boolean isUndirectedExpectation) {
        var type1 = RelationshipType.of("Foo");
        var type2 = RelationshipType.of("Bar");

        var relationshipSchema1 = RelationshipSchema.empty()
            .addProperty(type1, isUndirected1, "bar", ValueType.DOUBLE);

        var relationshipSchema2 = RelationshipSchema.empty()
            .addProperty(type2, isUndirected2, "bar", ValueType.DOUBLE);

        var expected = RelationshipSchema.empty()
            .addProperty(type1, isUndirected1, "bar", ValueType.DOUBLE)
            .addProperty(type2, isUndirected2, "bar", ValueType.DOUBLE);

        var actual = relationshipSchema1.union(relationshipSchema2);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.isUndirected()).isEqualTo(isUndirectedExpectation);

        assertThat(actual.get(type1).orientation()).isEqualTo(isUndirected1);
        assertThat(actual.get(type2).orientation()).isEqualTo(isUndirected2);
    }

    @Test
    void unionOnSameTypesFailsOnDirectionMismatch() {
        var schema1 = RelationshipSchema.empty()
            .addRelationshipType(RelationshipType.of("X"), NATURAL)
            .addRelationshipType(RelationshipType.of("Y"), UNDIRECTED);

        var schema2 = RelationshipSchema.empty()
            .addRelationshipType(RelationshipType.of("X"), UNDIRECTED)
            .addRelationshipType(RelationshipType.of("Y"), UNDIRECTED);

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Conflicting directionality for relationship types X");
    }


    @Test
    void unionOnSameTypeSamePropertyDifferentValueTypeFails() {
        var schema1 = RelationshipSchema.empty()
            .addProperty(RelationshipType.of("X"), NATURAL, "x", ValueType.DOUBLE)
            .addProperty(RelationshipType.of("Y"), UNDIRECTED, "unlikelypartofmessage", ValueType.DOUBLE);

        var schema2 = RelationshipSchema.empty()
            .addProperty(RelationshipType.of("X"), NATURAL, "x", ValueType.LONG)
            .addProperty(RelationshipType.of("Y"), UNDIRECTED, "unlikelypartofmessage", ValueType.DOUBLE);

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Combining schema entries with value type")
            .hasMessageContaining("x")
            .hasMessageContaining("DOUBLE")
            .hasMessageContaining("LONG")
            .hasMessageNotContaining("unlikelypartofmessage");
    }

    static Stream<Arguments> schemaAndHasProperties() {
        return Stream.of(
            Arguments.of(RelationshipSchema.empty().addRelationshipType(RelationshipType.of("A"), NATURAL), false),
            Arguments.of(RelationshipSchema.empty().addRelationshipType(RelationshipType.of("A"), UNDIRECTED), false),
            Arguments.of(RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("A"), NATURAL, "foo", ValueType.LONG), true),
            Arguments.of(RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("A"), UNDIRECTED, "foo", ValueType.LONG), true)
        );
    }

    @ParameterizedTest
    @MethodSource("schemaAndHasProperties")
    void shouldKnowIfPropertiesArePresent(RelationshipSchema relationshipSchema, boolean hasProperties) {
        assertThat(relationshipSchema.hasProperties()).isEqualTo(hasProperties);
    }

    @Test
    void testBuildRelTypes() {
        var schema = RelationshipSchema.empty();

        schema.addRelationshipType(RelationshipType.of("X"), UNDIRECTED);
        schema.addRelationshipType(RelationshipType.of("Y"), NATURAL);

        assertThat(schema.isUndirected()).isFalse();
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));
        assertThat(schema.availableTypes()).containsAll(List.of(RelationshipType.of("X"), RelationshipType.of("Y")));
        assertThat(schema.get(RelationshipType.of("X")).orientation()).isEqualTo(UNDIRECTED);
        assertThat(schema.get(RelationshipType.of("Y")).orientation()).isEqualTo(NATURAL);
    }

    @Test
    void testBuildProperties() {
        var schema = RelationshipSchema.empty()
            .addProperty(RelationshipType.of("X"), UNDIRECTED, "x", ValueType.DOUBLE)
            .addProperty(RelationshipType.of("Y"), NATURAL, "y", ValueType.DOUBLE, Aggregation.MIN)
            .addProperty(
                RelationshipType.of("Z"),
                UNDIRECTED,
                "z",
                RelationshipPropertySchema.of("z", ValueType.DOUBLE)
            );

        assertThat(schema.isUndirected()).isFalse();
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));

        assertThat(schema.get(RelationshipType.of("X")).properties()).containsExactlyInAnyOrderEntriesOf(Map.of("x", RelationshipPropertySchema.of("x", ValueType.DOUBLE)));
        assertThat(schema.get(RelationshipType.of("Y")).properties()).containsExactlyInAnyOrderEntriesOf(Map.of("y", RelationshipPropertySchema.of("y", ValueType.DOUBLE, Aggregation.MIN)));
        assertThat(schema.get(RelationshipType.of("Z")).properties()).containsExactlyInAnyOrderEntriesOf(Map.of("z", RelationshipPropertySchema.of("z", ValueType.DOUBLE)));


        assertThat(schema.get(RelationshipType.of("X")).orientation()).isEqualTo(UNDIRECTED);
        assertThat(schema.get(RelationshipType.of("Y")).orientation()).isEqualTo(NATURAL);
        assertThat(schema.get(RelationshipType.of("Z")).orientation()).isEqualTo(UNDIRECTED);
    }

    @Test
    void shouldCreateDeepCopiesWhenFiltering() {
        var relType = RelationshipType.of("A");
        var relationshipSchema = RelationshipSchema.empty();
        relationshipSchema.getOrCreateRelationshipType(relType, NATURAL).addProperty("prop", ValueType.LONG);
        var filteredSchema = relationshipSchema.filter(Set.of(relType));
        filteredSchema
            .getOrCreateRelationshipType(relType, NATURAL).addProperty("shouldNotExistInOriginalSchema", ValueType.LONG);

        assertThat(relationshipSchema.get(relType).properties())
            .doesNotContainKey("shouldNotExistInOriginalSchema")
            .containsOnlyKeys("prop");
    }
}
