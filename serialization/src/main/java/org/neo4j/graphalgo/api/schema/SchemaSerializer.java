/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.model.proto.GraphSchemaProto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class SchemaSerializer {
    private static final Set<DefaultValueSerializer> defaultValueSerializers = new HashSet<>();

    static {
        defaultValueSerializers.add(new DoubleDefaultValueSerializer());
        defaultValueSerializers.add(new LongDefaultValueSerializer());
        defaultValueSerializers.add(new DoubleArrayDefaultValueSerializer());
        defaultValueSerializers.add(new LongArrayDefaultValueSerializer());
        defaultValueSerializers.add(new FloatArrayDefaultValueSerializer());
    }

    private SchemaSerializer() {}

    public static GraphSchemaProto.GraphSchema serializableGraphSchema(GraphSchema graphSchema) {
        return GraphSchemaProto.GraphSchema.newBuilder()
            .putAllNodeSchema(serializableNodeSchema(graphSchema.nodeSchema()))
            .putAllRelationshipSchema(serializableRelationshipSchema(graphSchema.relationshipSchema()))
            .build();
    }

    public static Map<String, GraphSchemaProto.PropertyMapping> serializableNodeSchema(NodeSchema nodeSchema) {
        var serializableSchema = new HashMap<String, GraphSchemaProto.PropertyMapping>();
        nodeSchema.properties().forEach(((nodeLabel, properties) -> {
            var propertyMapping = GraphSchemaProto.PropertyMapping.newBuilder();
            var label = nodeLabel.name();
            properties.forEach((name, propertySchema) -> propertyMapping.putNameMapping(
                name,
                propertySchema(propertySchema)
            ));
            serializableSchema.put(label, propertyMapping.build());
        }));

        return serializableSchema;
    }

    private static GraphSchemaProto.PropertySchema propertySchema(org.neo4j.graphalgo.api.schema.PropertySchema propertySchema) {

        var propertySchemaBuilder = GraphSchemaProto.PropertySchema.newBuilder();
        var valueType = propertySchema.valueType();
        var defaultValue = propertySchema.defaultValue();

        GraphSchemaProto.DefaultValue.Builder defaultValueBuilder = getDefaultValueBuilder(valueType, defaultValue);

        return propertySchemaBuilder
            .setKey(propertySchema.key())
            .setValueType(valueType.name())
            .setDefaultValue(defaultValueBuilder)
            .setState(propertySchema.state().name())
            .build();
    }

    public static Map<String, GraphSchemaProto.RelationshipPropertyMapping> serializableRelationshipSchema(
        RelationshipSchema relationshipSchema
    ) {
        var serializableSchema = new HashMap<String, GraphSchemaProto.RelationshipPropertyMapping>();
        relationshipSchema.properties().forEach(((relationshipType, properties) -> {
            var schemaMapping = GraphSchemaProto.RelationshipPropertyMapping.newBuilder();
            properties.forEach(((propertyName, relationshipPropertySchema) -> {
                var relationshipPropertySchemaBuilder = GraphSchemaProto.RelationshipPropertySchema.newBuilder();
                relationshipPropertySchemaBuilder
                    .setPropertySchema(propertySchema(relationshipPropertySchema));
                relationshipPropertySchemaBuilder.setAggregation(relationshipPropertySchema.aggregation().name());
                schemaMapping.putTypeMapping(propertyName, relationshipPropertySchemaBuilder.build());
            }));

            serializableSchema.put(relationshipType.name(), schemaMapping.build());
        }));
        return serializableSchema;
    }

    private static GraphSchemaProto.DefaultValue.Builder getDefaultValueBuilder(
        ValueType valueType,
        DefaultValue defaultValue
    ) {
        var defaultValueBuilder = GraphSchemaProto.DefaultValue.newBuilder()
            .setIsUserDefined(defaultValue.isUserDefined());
        defaultValueSerializers.forEach(serializer -> {
            if (serializer.canProcess(valueType)) {
                serializer.processValue(defaultValue, defaultValueBuilder);
            }
        });

        return defaultValueBuilder;
    }

    interface DefaultValueSerializer {
        void processValue(DefaultValue defaultValue, GraphSchemaProto.DefaultValue.Builder defaultValueBuilder);

        boolean canProcess(ValueType valueType);
    }

    private static class LongDefaultValueSerializer implements DefaultValueSerializer {
        @Override
        public void processValue(DefaultValue defaultValue, GraphSchemaProto.DefaultValue.Builder defaultValueBuilder) {
            defaultValueBuilder.setLongValue(defaultValue.longValue());
        }

        @Override
        public boolean canProcess(ValueType valueType) {
            return ValueType.LONG == valueType;
        }
    }

    private static class DoubleDefaultValueSerializer implements DefaultValueSerializer {

        @Override
        public void processValue(DefaultValue defaultValue, GraphSchemaProto.DefaultValue.Builder defaultValueBuilder) {
            defaultValueBuilder.setDoubleValue(defaultValue.doubleValue());
        }

        @Override
        public boolean canProcess(ValueType valueType) {
            return ValueType.DOUBLE == valueType;
        }
    }

    private static class DoubleArrayDefaultValueSerializer implements DefaultValueSerializer {
        @Override
        public void processValue(DefaultValue defaultValue, GraphSchemaProto.DefaultValue.Builder defaultValueBuilder) {
            var doubleArrayValue = defaultValue.doubleArrayValue();
            if (doubleArrayValue != null) {
                var doubleArrayIterable = Arrays.stream(doubleArrayValue).boxed().collect(Collectors.toList());
                defaultValueBuilder.setDoubleArrayValue(GraphSchemaProto.DoubleArray
                    .newBuilder()
                    .addAllDoubleArrayValue(doubleArrayIterable));
            }
        }

        @Override
        public boolean canProcess(ValueType valueType) {
            return ValueType.DOUBLE_ARRAY == valueType;
        }
    }

    private static class LongArrayDefaultValueSerializer implements DefaultValueSerializer {
        @Override
        public void processValue(DefaultValue defaultValue, GraphSchemaProto.DefaultValue.Builder defaultValueBuilder) {
            var longArrayValue = defaultValue.longArrayValue();
            if (longArrayValue != null) {
                var longArrayIterable = Arrays.stream(longArrayValue).boxed().collect(Collectors.toList());
                defaultValueBuilder.setLongArrayValue(GraphSchemaProto.LongArray
                    .newBuilder()
                    .addAllLongArrayValue(longArrayIterable));
            }
        }

        @Override
        public boolean canProcess(ValueType valueType) {
            return ValueType.LONG_ARRAY == valueType;
        }
    }

    private static class FloatArrayDefaultValueSerializer implements DefaultValueSerializer {
        @Override
        public void processValue(DefaultValue defaultValue, GraphSchemaProto.DefaultValue.Builder defaultValueBuilder) {
            var floatArrayValue = defaultValue.floatArrayValue();
            if (floatArrayValue != null) {
                List<Float> floatArrayIterable = new ArrayList<>(floatArrayValue.length);
                for (int i = 0; i < floatArrayValue.length; i++) {
                    floatArrayIterable.add(i, floatArrayValue[i]);
                }
                defaultValueBuilder.setFloatArrayValue(GraphSchemaProto.FloatArray
                    .newBuilder()
                    .addAllFloatArrayValue(floatArrayIterable));
            }
        }

        @Override
        public boolean canProcess(ValueType valueType) {
            return ValueType.FLOAT_ARRAY == valueType;
        }
    }
}
