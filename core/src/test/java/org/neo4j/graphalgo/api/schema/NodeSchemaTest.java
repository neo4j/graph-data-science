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
package org.neo4j.graphalgo.api.schema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeSchemaTest {

    @Test
    void testDefaultValues() {
        var label = NodeLabel.of("Foo");

        DefaultValue defaultValue = DefaultValue.of(42.0D);
        String propertyName = "baz";
        var nodeSchema = NodeSchema.builder()
            .addProperty(
                label,
                propertyName,
                PropertySchema.of(
                    propertyName,
                    ValueType.DOUBLE,
                    defaultValue,
                    GraphStore.PropertyState.PERSISTENT
                )
            ).build();

        PropertySchema nodePropertySchema = nodeSchema.properties().get(label).get(propertyName);
        assertTrue(nodePropertySchema.defaultValue().isUserDefined());
        assertEquals(defaultValue, nodePropertySchema.defaultValue());
    }

    @Test
    void testFiltering() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .addProperty(label2, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(nodeSchema, nodeSchema.filter(Set.of(label1, label2)));

        var expected = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(expected, nodeSchema.filter(Set.of(label1)));
    }

    @Test
    void testUnion() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema1 = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .build();

        var nodeSchema2 = NodeSchema.builder()
            .addProperty(label2, "bar", ValueType.DOUBLE)
            .build();

        var expected = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label2, "bar", ValueType.DOUBLE)
            .build();

        assertEquals(expected, nodeSchema1.union(nodeSchema2));
    }

    @Test
    void testUnionSchema() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema1 = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addLabel(label2)
            .build();

        var nodeSchema2 = NodeSchema.builder()
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .addProperty(label2, "baz", ValueType.DOUBLE)
            .build();

        var expected = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.DOUBLE)
            .addProperty(label2, "baz", ValueType.DOUBLE)
            .build();

        assertEquals(expected, nodeSchema1.union(nodeSchema2));
    }

    @Test
    void testUnionOfIncompatibleProperties() {
        var label1 = NodeLabel.of("Foo");

        var nodeSchema1 = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.DOUBLE)
            .build();

        var nodeSchema2 = NodeSchema.builder()
            .addProperty(label1, "bar", ValueType.LONG)
            .build();

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> nodeSchema1.union(nodeSchema2)
        );
        assertTrue(ex
            .getMessage()
            .contains("Combining schema entries with value type {bar=DOUBLE} and {bar=LONG} is not supported."));
    }

    @Test
    void testUnionProperties() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema = NodeSchema.builder()
            .addProperty(label1, "foo", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.LONG)
            .addProperty(label2, "bar", ValueType.LONG_ARRAY)
            .addProperty(label2, "baz", ValueType.LONG)
            .build();

        var expectedUnionSchema = Map.of(
            "foo", PropertySchema.of("foo", ValueType.DOUBLE, DefaultValue.forDouble(), GraphStore.PropertyState.PERSISTENT),
            "bar", PropertySchema.of("bar", ValueType.LONG_ARRAY, DefaultValue.forLongArray(), GraphStore.PropertyState.PERSISTENT),
            "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), GraphStore.PropertyState.PERSISTENT)
        );

        var unionPropertySchema = nodeSchema.unionProperties();

        assertThat(unionPropertySchema).containsExactlyInAnyOrderEntriesOf(expectedUnionSchema);
    }

    @Test
    void testUnionPropertiesWithIncompatibleTypes() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema = NodeSchema.builder()
            .addProperty(label1, "foo", ValueType.DOUBLE)
            .addProperty(label2, "foo", ValueType.LONG)
            .build();

        var exception = assertThrows(IllegalArgumentException.class, nodeSchema::unionProperties);

        assertThat(exception).hasMessage("Combining schema entries with value type DOUBLE and LONG is not supported.");
    }

    static Stream<Arguments> schemaAndHasProperties() {
        return Stream.of(
            Arguments.of(NodeSchema.builder().addLabel(NodeLabel.of("A")).build(), false),
            Arguments.of(NodeSchema.builder().addProperty(NodeLabel.of("A"), "foo", ValueType.LONG).build(), true)
        );
    }

    @ParameterizedTest
    @MethodSource("schemaAndHasProperties")
    void shouldKnowIfPropertiesArePresent(NodeSchema nodeSchema, boolean hasProperties) {
        assertThat(nodeSchema.hasProperties()).isEqualTo(hasProperties);
    }
}
