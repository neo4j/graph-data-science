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

import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.model.proto.GraphSchemaProto;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    private static GraphSchemaProto.PropertySchema propertySchema(PropertySchema propertySchema) {

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

}
