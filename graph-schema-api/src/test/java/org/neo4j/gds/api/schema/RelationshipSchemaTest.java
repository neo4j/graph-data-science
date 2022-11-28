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
        undirectedSchema.getOrCreateRelationshipType(type, Direction.UNDIRECTED);
        assertThat(undirectedSchema.isUndirected()).isTrue();

        var directedSchema = RelationshipSchema.empty();
        directedSchema.getOrCreateRelationshipType(type, Direction.DIRECTED);
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
                Direction.DIRECTED,
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

    static Stream<Arguments> directions() {
        return Stream.of(
            Arguments.of(Direction.UNDIRECTED, true),
            Arguments.of(Direction.DIRECTED, false)
        );
    }

    @ParameterizedTest
    @MethodSource("directions")
    void testFiltering(Direction orientation, boolean isUndirected) {
        var label1 = RelationshipType.of("Foo");
        var label2 = RelationshipType.of("Bar");

        var relationshipSchema = RelationshipSchema.empty()
            .addProperty(label1, orientation, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT)
            .addProperty(label1, orientation, "baz", ValueType.DOUBLE, PropertyState.PERSISTENT)
            .addProperty(label2, orientation, "baz", ValueType.DOUBLE, PropertyState.PERSISTENT);

        assertThat(relationshipSchema.filter(Set.of(label1, label2))).isEqualTo(relationshipSchema);

        var expected = RelationshipSchema.empty()
            .addProperty(label1, orientation, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT)
            .addProperty(label1, orientation, "baz", ValueType.DOUBLE, PropertyState.PERSISTENT);

        assertThat(relationshipSchema.filter(Set.of(label1))).isEqualTo(expected);
        assertThat(relationshipSchema.isUndirected()).isEqualTo(isUndirected);
    }

    @Test
    void testFilteringMixed() {
        var directedType = RelationshipType.of("D");
        var undirectedType = RelationshipType.of("U");

        var directed = RelationshipSchema.empty()
            .addProperty(directedType, Direction.DIRECTED, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT);
        var undirected = RelationshipSchema.empty()
            .addProperty(undirectedType, Direction.UNDIRECTED, "flob", ValueType.DOUBLE, PropertyState.PERSISTENT);
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
            Arguments.of(Direction.UNDIRECTED, Direction.UNDIRECTED, true),
            Arguments.of(Direction.UNDIRECTED, Direction.DIRECTED, false),
            Arguments.of(Direction.DIRECTED, Direction.UNDIRECTED, false),
            Arguments.of(Direction.DIRECTED, Direction.DIRECTED, false)
        );
    }

    @ParameterizedTest
    @MethodSource("unionDirections")
    void testUnion(Direction direction1, Direction direction2, Boolean isUndirectedExpectation) {
        var type1 = RelationshipType.of("Foo");
        var type2 = RelationshipType.of("Bar");

        var relationshipSchema1 = RelationshipSchema.empty()
            .addProperty(type1, direction1, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT);

        var relationshipSchema2 = RelationshipSchema.empty()
            .addProperty(type2, direction2, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT);

        var expected = RelationshipSchema.empty()
            .addProperty(type1, direction1, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT)
            .addProperty(type2, direction2, "bar", ValueType.DOUBLE, PropertyState.PERSISTENT);

        var actual = relationshipSchema1.union(relationshipSchema2);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.isUndirected()).isEqualTo(isUndirectedExpectation);

        assertThat(actual.get(type1).direction()).isEqualTo(direction1);
        assertThat(actual.get(type2).direction()).isEqualTo(direction2);
    }

    @Test
    void unionOnSameTypesFailsOnDirectionMismatch() {
        var schema1 = RelationshipSchema.empty()
            .addRelationshipType(RelationshipType.of("X"), Direction.DIRECTED)
            .addRelationshipType(RelationshipType.of("Y"), Direction.UNDIRECTED);

        var schema2 = RelationshipSchema.empty()
            .addRelationshipType(RelationshipType.of("X"), Direction.UNDIRECTED)
            .addRelationshipType(RelationshipType.of("Y"), Direction.UNDIRECTED);

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Conflicting directionality for relationship types X");
    }


    @Test
    void unionOnSameTypeSamePropertyDifferentValueTypeFails() {
        var schema1 = RelationshipSchema
            .empty()
            .addProperty(RelationshipType.of("X"), Direction.DIRECTED, "x", ValueType.DOUBLE, PropertyState.PERSISTENT)
            .addProperty(RelationshipType.of("Y"),
                Direction.UNDIRECTED,
                "unlikelypartofmessage",
                ValueType.DOUBLE,
                PropertyState.PERSISTENT
            );

        var schema2 = RelationshipSchema
            .empty()
            .addProperty(RelationshipType.of("X"), Direction.DIRECTED, "x", ValueType.LONG, PropertyState.PERSISTENT)
            .addProperty(RelationshipType.of("Y"),
                Direction.UNDIRECTED,
                "unlikelypartofmessage",
                ValueType.DOUBLE,
                PropertyState.PERSISTENT
            );

        assertThatThrownBy(() -> schema1.union(schema2))
            .hasMessageContaining("Combining schema entries with value type")
            .hasMessageContaining("x")
            .hasMessageContaining("DOUBLE")
            .hasMessageContaining("LONG")
            .hasMessageNotContaining("unlikelypartofmessage");
    }

    static Stream<Arguments> schemaAndHasProperties() {
        return Stream.of(
            Arguments.of(RelationshipSchema.empty().addRelationshipType(RelationshipType.of("A"), Direction.DIRECTED),
                false
            ),
            Arguments.of(RelationshipSchema.empty().addRelationshipType(RelationshipType.of("A"), Direction.UNDIRECTED),
                false
            ),
            Arguments.of(RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("A"),
                    Direction.DIRECTED,
                    "foo",
                    ValueType.LONG,
                    PropertyState.PERSISTENT
                ), true),
            Arguments.of(RelationshipSchema
                .empty()
                .addProperty(RelationshipType.of("A"),
                    Direction.UNDIRECTED,
                    "foo",
                    ValueType.LONG,
                    PropertyState.PERSISTENT
                ), true)
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

        schema.addRelationshipType(RelationshipType.of("X"), Direction.UNDIRECTED);
        schema.addRelationshipType(RelationshipType.of("Y"), Direction.DIRECTED);

        assertThat(schema.isUndirected()).isFalse();
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));
        assertThat(schema.availableTypes()).containsAll(List.of(RelationshipType.of("X"), RelationshipType.of("Y")));
        assertThat(schema.get(RelationshipType.of("X")).direction()).isEqualTo(Direction.UNDIRECTED);
        assertThat(schema.get(RelationshipType.of("Y")).direction()).isEqualTo(Direction.DIRECTED);
    }

    @Test
    void testBuildProperties() {
        RelationshipSchema relationshipSchema = RelationshipSchema.empty()
            .addProperty(RelationshipType.of("X"), Direction.UNDIRECTED, "x", ValueType.DOUBLE,
                PropertyState.PERSISTENT
            );
        relationshipSchema
            .getOrCreateRelationshipType(RelationshipType.of("Y"), Direction.DIRECTED)
            .addProperty(
                "y",
                RelationshipPropertySchema.of(
                    "y",
                    ValueType.DOUBLE,
                    DefaultValue.forDouble(),
                    PropertyState.PERSISTENT,
                    Aggregation.MIN
                )
            );
        var schema = relationshipSchema
            .addProperty(
                RelationshipType.of("Z"),
                Direction.UNDIRECTED,
                "z",
                RelationshipPropertySchema.of("z", ValueType.DOUBLE)
            );

        assertThat(schema.isUndirected()).isFalse();
        assertThat(schema.availableTypes()).contains(RelationshipType.of("X"), RelationshipType.of("Y"));

        assertThat(schema.get(RelationshipType.of("X")).properties()).containsExactlyInAnyOrderEntriesOf(Map.of("x", RelationshipPropertySchema.of("x", ValueType.DOUBLE)));
        assertThat(schema.get(RelationshipType.of("Y")).properties()).containsExactlyInAnyOrderEntriesOf(Map.of("y", RelationshipPropertySchema.of("y", ValueType.DOUBLE, Aggregation.MIN)));
        assertThat(schema.get(RelationshipType.of("Z")).properties()).containsExactlyInAnyOrderEntriesOf(Map.of("z", RelationshipPropertySchema.of("z", ValueType.DOUBLE)));


        assertThat(schema.get(RelationshipType.of("X")).direction()).isEqualTo(Direction.UNDIRECTED);
        assertThat(schema.get(RelationshipType.of("Y")).direction()).isEqualTo(Direction.DIRECTED);
        assertThat(schema.get(RelationshipType.of("Z")).direction()).isEqualTo(Direction.UNDIRECTED);
    }

    @Test
    void shouldCreateDeepCopiesWhenFiltering() {
        var relType = RelationshipType.of("A");
        var relationshipSchema = RelationshipSchema.empty();
        relationshipSchema.getOrCreateRelationshipType(relType, Direction.DIRECTED).addProperty("prop", ValueType.LONG,
            PropertyState.PERSISTENT
        );
        var filteredSchema = relationshipSchema.filter(Set.of(relType));
        filteredSchema
            .getOrCreateRelationshipType(relType, Direction.DIRECTED).addProperty("shouldNotExistInOriginalSchema", ValueType.LONG,
                PropertyState.PERSISTENT
            );

        assertThat(relationshipSchema.get(relType).properties())
            .doesNotContainKey("shouldNotExistInOriginalSchema")
            .containsOnlyKeys("prop");
    }
}
