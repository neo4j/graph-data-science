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
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NodeSchemaTest {

    @Test
    void labelInsertionOrderDoesNotMatter() {
        var label = "LabelA";
        var propertyKey1 = "PropertyX";
        var propertyKey2 = "PropertyY";
        var valueType1 = ValueType.LONG;
        var valueType2 = ValueType.DOUBLE;

        var result1 = NodeSchema.builder()
            .addProperty(label, propertyKey1, valueType1)
            .addProperty(label, propertyKey2, valueType2)
            .build();
        var result2 = NodeSchema.builder()
            .addProperty(label, propertyKey2, valueType2)
            .addProperty(label, propertyKey1, valueType1)
            .build();

        assertThat(result1).isEqualTo(result2);
    }

    @Test
    void builderFiltersDuplicates() {
        var label = "LabelA";
        var propertyKey1 = "PropertyX";
        var propertyKey2 = "PropertyY";
        var valueType1 = ValueType.LONG;
        var valueType2 = ValueType.DOUBLE;

        var result1 = NodeSchema.builder()
            .addProperty(label, propertyKey2, valueType2)
            .addProperty(label, propertyKey1, valueType1)
            .addProperty(label, propertyKey2, valueType2)
            .addProperty(label, propertyKey2, valueType2)
            .addProperty(label, propertyKey1, valueType1)
            .build();
        var result2 = NodeSchema.builder()
            .addProperty(label, propertyKey1, valueType1)
            .addProperty(label, propertyKey2, valueType2)
            .build();

        assertThat(result1).isEqualTo(result2);
    }

    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.EXCLUDE, names = {"FLOAT_VECTOR", "DOUBLE_VECTOR"})
    void buildingWithAllValueTypes(ValueType valueType) {
        var label = "LabelA";
        var propertyKey = "Property" + valueType.name();

        var result = NodeSchema.builder()
            .addProperty(label, propertyKey, valueType)
            .build();

        var expected = createSchema(
            SchemaEntry.of(label, propertyKey, valueType)
        );
        assertThat(result).isEqualTo(expected);
    }

    @ParameterizedTest
    @EnumSource(mode = EnumSource.Mode.INCLUDE, names = {"FLOAT_VECTOR", "DOUBLE_VECTOR"})
    void buildingWithVectorValueTypesRequiresADimension(ValueType valueType) {
        var label = "LabelA";
        var propertyKey = "Property" + valueType.name();

        assertThrows(
            IllegalArgumentException.class,
            () -> NodeSchema.builder().addProperty(label, propertyKey, valueType).build()
        );

        var result = NodeSchema.builder()
            .addProperty(
                label,
                PropertySchema.of(
                    propertyKey,
                    valueType,
                    valueType.fallbackValue(),
                    PropertyState.PERSISTENT,
                    OptionalInt.of(42)
                )
            )
            .build();

        assertThat(result.propertiesForLabel(NodeLabel.of(label)).get(propertyKey).dimension()).hasValue(42);
    }

    @Test
    void buildingFromEntireSchema() {
        var label = "LabelA";
        var propertyKey1 = "PropertyX";
        var propertyKey2 = "PropertyY";
        var valueType = ValueType.LONG;
        var schemaWithLabel1 = NodeSchema.builder().addProperty(label, propertyKey1, valueType).build();

        var result = NodeSchema.builder()
            .addProperty(label, propertyKey2, valueType)
            .addSchema(schemaWithLabel1)
            .build();

        var expected = createSchema(
            SchemaEntry.of(label, propertyKey1, valueType),
            SchemaEntry.of(label, propertyKey2, valueType)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void buildingWithOnlyLabel() {
        var label = "LabelA";

        var result = NodeSchema.builder().addLabel(label).build();

        var expected = createSchema(
            SchemaEntry.of(label)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void builderFiltersDuplicateLabels() {
        var label = "LabelA";

        var result = NodeSchema.builder()
            .addLabel(label)
            .addLabel(label)
            .addLabel(label)
            .build();

        var expected = createSchema(
            SchemaEntry.of(label)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void buildingWithPropertyLessLabelInDifferentOrder() {
        var label = "LabelA";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;

        var result1 = NodeSchema.builder()
            .addLabel(label)
            .addProperty(label, propertyKey, valueType)
            .build();
        var result2 = NodeSchema.builder()
            .addProperty(label, propertyKey, valueType)
            .addLabel(label)
            .build();

        var expected = createSchema(
            SchemaEntry.of(label, propertyKey, valueType)
        );
        assertThat(result1).isEqualTo(expected);
        assertThat(result2).isEqualTo(expected);
    }

    @Test
    void buildingWithAnotherBuilder() {
        var builder1 = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelB", "PropertyY", ValueType.DOUBLE);
        var builder2 = NodeSchema.builder()
            .addProperty("LabelB", "PropertyY", ValueType.DOUBLE)
            .addProperty("LabelC", "PropertyZ", ValueType.LONG);

        var result = builder1.addBuilder(builder2).build();

        var expected = createSchema(
            SchemaEntry.of("LabelA", "PropertyX", ValueType.LONG),
            SchemaEntry.of("LabelB", "PropertyY", ValueType.DOUBLE),
            SchemaEntry.of("LabelC", "PropertyZ", ValueType.LONG)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void buildingWithSelf() {
        var builder1 = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelB", "PropertyY", ValueType.DOUBLE);

        var result = builder1.addBuilder(builder1).build();

        var expected = createSchema(
            SchemaEntry.of("LabelA", "PropertyX", ValueType.LONG),
            SchemaEntry.of("LabelB", "PropertyY", ValueType.DOUBLE)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void readDefaultValue() {
        var label = "LabelA";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;
        var defaultvalue = DefaultValue.of(42L);
        var propertyState = PropertyState.PERSISTENT;
        var schema = NodeSchema.builder()
            .addProperty(label, propertyKey, valueType, defaultvalue, propertyState)
            .build();

        var result = schema.getDefaultValueFor(label, propertyKey);

        assertThat(result).isEqualTo(Optional.of(defaultvalue));
    }

    @Test
    void readDefaultValueFromNonExistingLabel() {
        var label = "LabelA";
        var propertyKey = "PropertyX";
        var schema = NodeSchema.empty();

        var result = schema.getDefaultValueFor(label, propertyKey);

        assertThat(result).isEqualTo(Optional.empty());
    }

    @Test
    void readDefaultValueFromNonExistingProperty() {
        var label = "LabelA";
        var propertyKey = "PropertyX";
        var schema = NodeSchema.builder()
            .addLabel(label)
            .build();

        var result = schema.getDefaultValueFor(label, propertyKey);

        assertThat(result).isEqualTo(Optional.empty());
    }

    @Test
    void filterSchemaWithSubsetOfLabels() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;
        var schema = NodeSchema.builder()
            .addProperty(label1, propertyKey, valueType)
            .addProperty(label2, propertyKey, valueType)
            .build();

        var result = schema.filter(Set.of(NodeLabel.of(label1)));

        var expected = createSchema(
            SchemaEntry.of(label1, propertyKey, valueType)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void filterSchemaWithAllLabels() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;
        var schema = NodeSchema.builder()
            .addProperty(label1, propertyKey, valueType)
            .addProperty(label2, propertyKey, valueType)
            .build();

        var result = schema.filter(Set.of(NodeLabel.of(label1), NodeLabel.of(label2)));

        assertThat(result).isEqualTo(schema);
    }

    @Test
    void filterOutSchemaEntirely() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;
        var schema = NodeSchema.builder()
            .addProperty(label1, propertyKey, valueType)
            .addProperty(label2, propertyKey, valueType)
            .build();

        var result = schema.filter(Set.of());

        var expected = createSchema();
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void clientNotAllowedToMutateNodeSchemasEntries() {
        assertThrows(
            UnsupportedOperationException.class, () ->
                new NodeSchema(new HashMap<>()).entries()
                    .put(NodeLabel.of("LabelA"), List.of())
        );
    }

    @Test
    void clientNotAllowedToMutateNodeSchemasProperties() {
        var entries = new HashMap<NodeLabel, List<PropertySchema>>();
        var label1 = NodeLabel.of("LabelA");
        entries.put(label1, new ArrayList<>());
        var schema = new NodeSchema(entries);

        assertThrows(
            UnsupportedOperationException.class, () ->
                schema.entries()
                    .get(label1)
                    .add(PropertySchema.of("BAR", ValueType.DOUBLE))
        );
    }

    @Test
    void clientNotAllowedToMutateNodeSchemaFromBuilder() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;
        var schema = NodeSchema.builder()
            .addProperty(label1, propertyKey, valueType)
            .addProperty(label2, propertyKey, valueType)
            .build();

        assertThrows(
            UnsupportedOperationException.class, () ->
                schema.entries()
                    .get(NodeLabel.of(label1))
                    .add(PropertySchema.of("BAR", ValueType.DOUBLE))
        );
    }

    @Test
    void clientNotAllowedToMutateFilteredNodeSchema() {
        var label1 = NodeLabel.of("LabelA");
        var entries = new HashMap<NodeLabel, List<PropertySchema>>();
        var propertySchemas = new ArrayList<PropertySchema>();
        entries.put(label1, propertySchemas);
        entries.put(NodeLabel.of("LabelB"), new ArrayList<>());

        var schema = new NodeSchema(entries);
        var filteredSchema = schema.filter(Set.of(NodeLabel.of("LabelA")));

        // Changing original map should not change the schema, nor the filtered schema
        entries.put(NodeLabel.of("LabelC"), List.of());
        assertThat(entries.size()).isEqualTo(3);
        assertThat(schema.entries().size()).isEqualTo(2);
        assertThat(filteredSchema.entries().size()).isEqualTo(1);

        // Changing original property list should not change the schema, nor the filtered schema
        propertySchemas.add(PropertySchema.of("BAR", ValueType.DOUBLE));
        assertThat(entries.get(label1).size()).isEqualTo(1);
        assertThat(schema.entries().get(label1).size()).isEqualTo(0);
        assertThat(filteredSchema.entries().get(label1).size()).isEqualTo(0);
    }

    @Test
    void unionBetweenSchemasWithDifferentLabels() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var propertyKey = "PropertyX";
        var valueType = ValueType.LONG;
        var schemaWithLabel1 = NodeSchema.builder().addProperty(label1, propertyKey, valueType).build();
        var schemaWithLabel2 = NodeSchema.builder().addProperty(label2, propertyKey, valueType).build();

        var result = schemaWithLabel1.union(schemaWithLabel2);

        var expected = createSchema(
            SchemaEntry.of(label1, propertyKey, valueType),
            SchemaEntry.of(label2, propertyKey, valueType)
        );
        assertThat(schemaWithLabel1.entries().size()).isEqualTo(1);
        assertThat(schemaWithLabel2.entries().size()).isEqualTo(1);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void unionBetweenSchemasWithDifferentProperties() {
        var label = "LabelA";
        var propertyKey1 = "PropertyX";
        var propertyKey2 = "PropertyY";
        var valueType = ValueType.LONG;
        var schemaWithPropertyKey1 = NodeSchema.builder().addProperty(label, propertyKey1, valueType).build();
        var schemaWithPropertyKey2 = NodeSchema.builder().addProperty(label, propertyKey2, valueType).build();

        var result = schemaWithPropertyKey1.union(schemaWithPropertyKey2);

        var expected = createSchema(
            SchemaEntry.of(label, propertyKey1, valueType),
            SchemaEntry.of(label, propertyKey2, valueType)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void unionBetweenSchemasWithSamePropertyButDifferentTypeIsInvalid() {
        var label = "LabelA";
        var propertyKey = "PropertyX";
        var schemaWithValueType1 = NodeSchema.builder().addProperty(label, propertyKey, ValueType.LONG).build();
        var schemaWithValueType2 = NodeSchema.builder().addProperty(label, propertyKey, ValueType.DOUBLE).build();

        assertThatThrownBy(() -> schemaWithValueType1.union(schemaWithValueType2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Combining schema entries with value type");
    }

    @Test
    void unionBetweenSchemasWithSamePropertyButDifferentDimensionsIsInvalid() {
        var label = "LabelA";
        var propertyKey = "PropertyX";
        var schemaWithValueType1 = NodeSchema.builder().addProperty(label, PropertySchema.of(propertyKey, ValueType.FLOAT_VECTOR, DefaultValue.forFloatArray(), PropertyState.TRANSIENT, OptionalInt.of(4))).build();
        var schemaWithValueType2 = NodeSchema.builder().addProperty(label, PropertySchema.of(propertyKey, ValueType.FLOAT_VECTOR, DefaultValue.forFloatArray(), PropertyState.TRANSIENT, OptionalInt.of(5))).build();

        assertThatThrownBy(() -> schemaWithValueType1.union(schemaWithValueType2))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Combining schema entries with different dimensions");
    }

    @Test
    void unionBetweenSchemasWithDifferentPropertyAndDifferentDimensionsIsValid() {
        var schemaWithValueType1 = NodeSchema.builder().addProperty("labelA", PropertySchema.of("propertyX", ValueType.FLOAT_VECTOR, DefaultValue.forFloatArray(), PropertyState.TRANSIENT, OptionalInt.of(313))).build();
        var schemaWithValueType2 = NodeSchema.builder().addProperty("labelB", PropertySchema.of("propertyY", ValueType.FLOAT_VECTOR, DefaultValue.forFloatArray(), PropertyState.TRANSIENT, OptionalInt.of(313))).build();

        assertThatNoException().isThrownBy(() -> schemaWithValueType1.union(schemaWithValueType2));
    }

    @Test
    void schemaHasNoPropertiesWhenEmpty() {
        var schema = NodeSchema.empty();

        var result = schema.hasProperties();

        assertThat(result).isFalse();
    }

    @Test
    void schemaHasNoPropertiesWhenLabelsHaveNoProperties() {
        var schema = NodeSchema.builder().addLabel("LabelA").build();

        var result = schema.hasProperties();

        assertThat(result).isFalse();
    }

    @Test
    void schemaHasProperties() {
        var schema1 = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .build();
        var schema2 = NodeSchema.builder()
            .addLabel("LabelA")
            .addProperty("LabelB", "PropertyX", ValueType.LONG)
            .build();

        assertThat(schema1.hasProperties()).isTrue();
        assertThat(schema2.hasProperties()).isTrue();
    }

    @Test
    void listAllLabelsWhenEmpty() {
        var schema = NodeSchema.empty();

        var result = schema.availableLabels();

        assertThat(result).isEqualTo(Set.of());
    }

    @Test
    void listAllLabels() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var schema = NodeSchema.builder()
            .addProperty(label1, "PropertyX", ValueType.LONG)
            .addProperty(label2, "PropertyX", ValueType.LONG)
            .build();

        var result = schema.availableLabels();

        var expected = Set.of(NodeLabel.of(label1), NodeLabel.of(label2));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void allLabelsSchema() {
        var schema1 = NodeSchema.builder().addLabel(NodeLabel.ALL_LABEL).build();
        var schema2 = NodeSchema.empty();

        assertThat(schema1.containsOnlyAllNodesLabel()).isTrue();
        assertThat(schema2.containsOnlyAllNodesLabel()).isFalse();
    }

    @Test
    void mixingArbitraryLabelWithAllLabel() {
        var labelA = "LabelA";
        var allLabel = NodeLabel.ALL_LABEL;

        var schema1 = NodeSchema.builder().addLabel(labelA).addLabel(allLabel).build();
        var schema2 = NodeSchema.builder().addLabel(allLabel).addLabel(labelA).build();

        var expected = Set.of(NodeLabel.of(labelA), NodeLabel.of(allLabel));
        assertThat(schema1.availableLabels()).isEqualTo(expected);
        assertThat(schema2.availableLabels()).isEqualTo(expected);
    }

    @Test
    void getAllPropertiesWhenEmpty() {
        var schema = NodeSchema.empty();

        var result = schema.allProperties();

        assertThat(result).isEqualTo(List.of());
    }

    @Test
    void getAllProperties() {
        var property1 = "PropertyX";
        var property2 = "PropertyY";
        var property3 = "PropertyZ";
        var schema = NodeSchema.builder()
            .addLabel("LabelA")
            .addProperty("LabelB", property1, ValueType.LONG)
            .addProperty("LabelC", property2, ValueType.LONG)
            .addProperty("LabelC", property3, ValueType.LONG)
            .build();

        var result = schema.allProperties();
        var resultingKeys = result.stream()
            .map(PropertySchema::key)
            .sorted()
            .toList();

        var expected = List.of(property1, property2, property3);
        assertThat(resultingKeys).isEqualTo(expected);
    }

    @Test
    void getAllPropertiesWithDuplicates() {
        var property1 = "PropertyX";
        var property2 = "PropertyY";
        var schema = NodeSchema.builder()
            .addLabel("LabelA")
            .addProperty("LabelB", property1, ValueType.LONG)
            .addProperty("LabelC", property1, ValueType.LONG)
            .addProperty("LabelC", property2, ValueType.LONG)
            .build();

        var result = schema.allProperties();
        var resultingKeys = result.stream()
            .map(PropertySchema::key)
            .sorted()
            .toList();

        var expected = List.of(property1, property2);
        assertThat(resultingKeys).isEqualTo(expected);
    }

    @Test
    void getAllPropertyKeysWithLabel() {
        var label1 = "LabelA";
        var label2 = "LabelB";
        var property1 = "PropertyX";
        var property2 = "PropertyY";
        var property3 = "PropertyZ";
        var schema = NodeSchema.builder()
            .addProperty(label1, property1, ValueType.LONG)
            .addProperty(label1, property2, ValueType.LONG)
            .addProperty(label2, property3, ValueType.LONG)
            .build();

        var result = schema.allPropertyKeysWithLabel(label1);

        var expected = Set.of(property1, property2);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void getAllPropertyKeysWithNonExistingLabel() {
        var schema = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelA", "PropertyY", ValueType.LONG)
            .addProperty("LabelB", "PropertyZ", ValueType.LONG)
            .build();

        var result = schema.allPropertyKeysWithLabel("FOO");

        assertThat(result).isEqualTo(Set.of());
    }

    @Test
    void containsPropertyKeyForLabel() {
        var label = "LabelA";
        var property = "PropertyX";
        var schema = NodeSchema.builder()
            .addProperty(label, property, ValueType.LONG)
            .build();

        assertThat(schema.hasProperty(label, property)).isTrue();
    }

    @Test
    void doesNotContainPropertyKeyForLabel() {
        var schema = NodeSchema.empty();

        assertThat(schema.hasProperty("FOO", "BAR")).isFalse();
    }

    @Test
    void getPropertyKeysMappedToSchema() {
        var schema = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelA", "PropertyY", ValueType.LONG)
            .addProperty("LabelB", "PropertyZ", ValueType.LONG)
            .build();

        var result = schema.properties();

        var expected = Map.of(
            "PropertyX", PropertySchema.of("PropertyX", ValueType.LONG),
            "PropertyY", PropertySchema.of("PropertyY", ValueType.LONG),
            "PropertyZ", PropertySchema.of("PropertyZ", ValueType.LONG)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void duplicatePropertyKeysWillOnlyMapToOneSchema() {
        var schema = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelB", "PropertyX", ValueType.LONG)
            .build();

        var result = schema.properties();

        var expected = Map.of("PropertyX", PropertySchema.of("PropertyX", ValueType.LONG));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void getPropertyKeysMappedToSchemaForLabel() {
        var schema = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelA", "PropertyY", ValueType.LONG)
            .addProperty("LabelB", "PropertyZ", ValueType.LONG)
            .build();

        var result = schema.propertiesForLabel(NodeLabel.of("LabelA"));

        var expected = Map.of(
            "PropertyX", PropertySchema.of("PropertyX", ValueType.LONG),
            "PropertyY", PropertySchema.of("PropertyY", ValueType.LONG)
        );
        assertThat(result).isEqualTo(expected);
    }

    @Test
    void getPropertyKeysMappedToSchemaForLabelNotInSchema() {
        var schema = NodeSchema.builder()
            .addProperty("LabelA", "PropertyX", ValueType.LONG)
            .addProperty("LabelA", "PropertyY", ValueType.LONG)
            .addProperty("LabelB", "PropertyZ", ValueType.LONG)
            .build();

        var result = schema.propertiesForLabel(NodeLabel.of("Foo"));

        assertThat(result).isEqualTo(Collections.emptyMap());
    }

    private static NodeSchema createSchema(SchemaEntry... entries) {
        return NodeSchema.of(
            Arrays.stream(entries).collect(
                Collectors.groupingBy(
                    row -> NodeLabel.of(row.label()),
                    Collectors.flatMapping(
                        row -> row.toPropertySchema().stream(),
                        Collectors.toList()
                    )
                )
            )
        );
    }
}
