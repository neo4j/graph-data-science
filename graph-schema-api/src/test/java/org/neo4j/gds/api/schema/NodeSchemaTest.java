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
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeSchemaTest {

    @Test
    void handlesOutsideOfSchemaRequests() {
        NodeSchema empty = NodeSchema.empty();
        assertFalse(empty.hasProperty(NodeLabel.of("NotInSchema"), "notInSchemaEither"));
    }

    @Test
    void testDefaultValues() {
        var label = NodeLabel.of("Foo");

        DefaultValue defaultValue = DefaultValue.of(42.0D);
        String propertyName = "baz";
        var nodeSchema = NodeSchema.empty();
        nodeSchema
            .getOrCreateLabel(label)
            .addProperty(
                propertyName,
                PropertySchema.of(propertyName, ValueType.DOUBLE, defaultValue, PropertyState.PERSISTENT)
            );

        PropertySchema nodePropertySchema = nodeSchema.get(label).properties().get(propertyName);
        assertTrue(nodePropertySchema.defaultValue().isUserDefined());
        assertEquals(defaultValue, nodePropertySchema.defaultValue());
    }

    @Test
    void testFiltering() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema = NodeSchema.empty();
        nodeSchema.getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE).addProperty("baz", ValueType.DOUBLE);
        nodeSchema.getOrCreateLabel(label2).addProperty("baz", ValueType.DOUBLE);

        assertEquals(nodeSchema, nodeSchema.filter(Set.of(label1, label2)));

        var expected = NodeSchema.empty();
        expected.getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE).addProperty("baz", ValueType.DOUBLE);

        assertEquals(expected, nodeSchema.filter(Set.of(label1)));
    }

    @Test
    void testUnion() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema1 = NodeSchema.empty();
        nodeSchema1.getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE);

        var nodeSchema2 = NodeSchema.empty();
        nodeSchema2.getOrCreateLabel(label2).addProperty("bar", ValueType.DOUBLE);

        var expected = NodeSchema.empty();
        expected.getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE);
        expected.getOrCreateLabel(label2).addProperty("bar", ValueType.DOUBLE);

        assertEquals(expected, nodeSchema1.union(nodeSchema2));
    }

    @Test
    void testUnionSchema() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema1 = NodeSchema.empty();
        nodeSchema1.getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE);
        nodeSchema1.getOrCreateLabel(label2);

        var nodeSchema2 = NodeSchema.empty();
        nodeSchema2.getOrCreateLabel(label1).addProperty("baz", ValueType.DOUBLE);
        nodeSchema2.getOrCreateLabel(label2).addProperty("baz", ValueType.DOUBLE);

        var expected = NodeSchema.empty();
        expected.getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE).addProperty("baz", ValueType.DOUBLE);
        expected.getOrCreateLabel(label2).addProperty("baz", ValueType.DOUBLE);

        assertEquals(expected, nodeSchema1.union(nodeSchema2));
    }

    @Test
    void testUnionOfIncompatibleProperties() {
        var label1 = NodeLabel.of("Foo");

        var nodeSchema1 = NodeSchema.empty()
            .getOrCreateLabel(label1).addProperty("bar", ValueType.DOUBLE);

        var nodeSchema2 = NodeSchema.empty()
            .getOrCreateLabel(label1).addProperty("bar", ValueType.LONG);

        var ex = assertThrows(IllegalArgumentException.class, () -> nodeSchema1.union(nodeSchema2));
        assertTrue(ex
            .getMessage()
            .contains("Combining schema entries with value type {bar=DOUBLE} and {bar=LONG} is not supported."));
    }

    @Test
    void testUnionProperties() {
        var label1 = NodeLabel.of("Foo");
        var label2 = NodeLabel.of("Bar");

        var nodeSchema = NodeSchema.empty()
            .addProperty(label1, "foo", ValueType.DOUBLE)
            .addProperty(label1, "baz", ValueType.LONG)
            .addProperty(label2, "bar", ValueType.LONG_ARRAY)
            .addProperty(label2, "baz", ValueType.LONG);

        var expectedUnionSchema = Map.of(
            "foo",
            PropertySchema.of("foo", ValueType.DOUBLE, DefaultValue.forDouble(), PropertyState.PERSISTENT),
            "bar",
            PropertySchema.of("bar", ValueType.LONG_ARRAY, DefaultValue.forLongArray(), PropertyState.PERSISTENT),
            "baz",
            PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.PERSISTENT)
        );

        var unionPropertySchema = nodeSchema.unionProperties();

        assertThat(unionPropertySchema).containsExactlyInAnyOrderEntriesOf(expectedUnionSchema);
    }

    @Test
    void shouldCreateDeepCopiesWhenFiltering() {
        var nodeLabel = NodeLabel.of("A");
        var nodeSchema = NodeSchema.empty();
        nodeSchema.getOrCreateLabel(nodeLabel).addProperty("prop", ValueType.LONG);
        var filteredNodeSchema = nodeSchema.filter(Set.of(nodeLabel));
        filteredNodeSchema
            .getOrCreateLabel(nodeLabel).addProperty("shouldNotExistInOriginalSchema", ValueType.LONG);

        assertThat(nodeSchema.get(nodeLabel).properties())
            .doesNotContainKey("shouldNotExistInOriginalSchema")
            .containsOnlyKeys("prop");
    }

    static Stream<Arguments> schemaAndHasProperties() {
        NodeSchema.empty();

        return Stream.of(
            Arguments.of(NodeSchema.empty().addLabel(NodeLabel.of("A")), false),
            Arguments.of(NodeSchema.empty().addProperty(NodeLabel.of("A"), "foo", ValueType.LONG), true)
        );
    }

    @ParameterizedTest
    @MethodSource("schemaAndHasProperties")
    void shouldKnowIfPropertiesArePresent(NodeSchema nodeSchema, boolean hasProperties) {
        assertThat(nodeSchema.hasProperties()).isEqualTo(hasProperties);
    }

    @Test
    void shouldCopyUnionPropertiesToNodeLabel() {
        var label = NodeLabel.of("Foo");

        var nodeSchema = NodeSchema.empty()
            .addProperty(label, "foo", ValueType.DOUBLE)
            .addProperty(label, "baz", PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT))
            .addProperty(label, "bar", ValueType.LONG_ARRAY);

        nodeSchema.addLabel(NodeLabel.of("Test"));
        nodeSchema.copyUnionPropertiesToLabel(NodeLabel.of("Test"));
        var propertySchemas = nodeSchema.propertySchemasFor(NodeLabel.of("Test"));
        assertThat(propertySchemas).containsExactlyInAnyOrder(
            PropertySchema.of("foo", ValueType.DOUBLE),
            PropertySchema.of("baz", ValueType.LONG, DefaultValue.forLong(), PropertyState.TRANSIENT),
            PropertySchema.of("bar", ValueType.LONG_ARRAY)
        );
    }
}
