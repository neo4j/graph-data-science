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

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.api.schema.NodeSchema;
import org.neo4j.graphalgo.api.schema.PropertySchema;
import org.neo4j.graphalgo.api.schema.RelationshipPropertySchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.model.proto.GraphSchemaProto;

import static org.neo4j.graphalgo.api.nodeproperties.ValueType.valueOf;

public final class SchemaDeserializer {
    private SchemaDeserializer() {}

    public static GraphSchema graphSchema(GraphSchemaProto.GraphSchema serializableGraphSchema) {
        return GraphSchema.of(
            nodeSchema(serializableGraphSchema.getNodeSchemaMap()),
            relationshipSchema(serializableGraphSchema.getRelationshipSchemaMap())
        );
    }

    public static NodeSchema nodeSchema(java.util.Map<String, GraphSchemaProto.PropertyMapping> nodeSchemaMap) {
        var nodeSchemaBuilder = NodeSchema.builder();
        nodeSchemaMap.forEach(((serializedLabel, propertyMapping) -> {
            var nodeLabel = NodeLabel.of(serializedLabel);
            var serializedPropertyMapping = propertyMapping.getNameMappingMap();
            serializedPropertyMapping.forEach(((propertyName, propertySchema) -> {
                var valueType = valueOf(propertySchema.getValueType());
                var defaultValue = propertySchema.getDefaultValue();
                var mappedPropSchema = PropertySchema.of(
                    propertyName,
                    valueType,
                    SchemaDeserializer.defaultValue(valueType, defaultValue),
                    GraphStore.PropertyState.valueOf(propertySchema.getState())
                );
                nodeSchemaBuilder.addProperty(nodeLabel, propertyName, mappedPropSchema);
            }));
        }));
        return nodeSchemaBuilder.build();
    }

    public static RelationshipSchema relationshipSchema(java.util.Map<String, GraphSchemaProto.RelationshipPropertyMapping> relationshipSchemaMap) {
        var relationshipSchemaBuilder = RelationshipSchema.builder();
        relationshipSchemaMap.forEach(((serializedType, propertyMapping) -> {
            var relationshipType = RelationshipType.of(serializedType);
            var serializedPropertyMapping = propertyMapping.getTypeMappingMap();
            if (serializedPropertyMapping.isEmpty()) {
                relationshipSchemaBuilder.addRelationshipType(RelationshipType.ALL_RELATIONSHIPS);
            } else {
                serializedPropertyMapping.forEach(((propertyName, propertySchemaMapping) -> {
                    var propertySchema = propertySchemaMapping.getPropertySchema();
                    var valueType = valueOf(propertySchema.getValueType());
                    var defaultValue = propertySchema.getDefaultValue();
                    var mappedPropSchema = RelationshipPropertySchema.of(
                        propertyName,
                        valueType,
                        SchemaDeserializer.defaultValue(valueType, defaultValue),
                        GraphStore.PropertyState.valueOf(propertySchema.getState()),
                        Aggregation.valueOf(propertySchemaMapping.getAggregation())
                    );
                    relationshipSchemaBuilder.addProperty(relationshipType, propertyName, mappedPropSchema);
                }));
            }
        }));
        return relationshipSchemaBuilder.build();
    }

    public static DefaultValue defaultValue(ValueType valueType, GraphSchemaProto.DefaultValue serializedDefaultValue) {
        Object value = serializedDefaultValue(valueType, serializedDefaultValue);

        return DefaultValue.of(value, serializedDefaultValue.getIsUserDefined());
    }

    static @Nullable Object serializedDefaultValue(
        ValueType valueType,
        GraphSchemaProto.DefaultValue serializedDefaultValue
    ) {
        Object value;
        switch (valueType) {
            case LONG:
                value = serializedDefaultValue.getLongValue();
                break;
            case DOUBLE:
                value = serializedDefaultValue.getDoubleValue();
                break;
            case DOUBLE_ARRAY:
                value = serializedDefaultValue.getDoubleArrayValue().getHasValue() ?
                    serializedDefaultValue.getDoubleArrayValue().getDoubleArrayValueList() :
                    DefaultValue.forDoubleArray().doubleArrayValue();
                break;
            case FLOAT_ARRAY:
                value = serializedDefaultValue.getFloatArrayValue().getHasValue() ?
                    serializedDefaultValue.getFloatArrayValue().getFloatArrayValueList() :
                    DefaultValue.forFloatArray().floatArrayValue();
                break;
            case LONG_ARRAY:
                value = serializedDefaultValue.getLongArrayValue().getHasValue() ?
                    serializedDefaultValue.getLongArrayValue().getLongArrayValueList() :
                    DefaultValue.forLongArray().longArrayValue();
                break;
            case UNKNOWN:
            default:
                value = null;
                break;
        }
        return value;
    }
}
