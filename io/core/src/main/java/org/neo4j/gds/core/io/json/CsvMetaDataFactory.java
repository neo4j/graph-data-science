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
package org.neo4j.gds.core.io.json;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.ImmutableDatabaseInfo;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.MutableNodeSchemaEntry;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.file.GraphInfo;
import org.neo4j.gds.core.io.file.GraphInfoBuilder;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public final class CsvMetaDataFactory {
    private CsvMetaDataFactory() {}

    static GraphInfo toGraphInfo(GraphStoreMetadata graphStoreMetadata) {
        var builder = GraphInfoBuilder
            .builder()
            .databaseInfo(toDatabaseInfo(graphStoreMetadata))
            .idMapBuilderType(graphStoreMetadata.idMapInfo().idMapType())
            .nodeCount(graphStoreMetadata.idMapInfo().nodeCount())
            .maxOriginalId(graphStoreMetadata.idMapInfo().maxOriginalId())
            .relationshipTypeCounts(toRelationshipTypeCounts(graphStoreMetadata))
            .inverseIndexedRelationshipTypes(toInverseRelationshipTypes(graphStoreMetadata));
        return builder.build();
    }

    static Capabilities toCapabilities(GraphStoreMetadata graphStoreMetadata) {
        return ImmutableStaticCapabilities
            .builder()
            .writeMode(toWriteMode(graphStoreMetadata))
            .build();
    }

    static org.neo4j.gds.api.schema.NodeSchema toNodeSchema(GraphStoreMetadata graphStoreMetadata) {
        var result = MutableNodeSchema.empty();

        var nodeSchemaEntries = graphStoreMetadata
            .nodeSchema()
            .entrySet()
            .stream()
            .map(CsvMetaDataFactory::toNodeSchemaEntry)
            .toList();
        nodeSchemaEntries.forEach(result::set);

        return result;
    }

    private static MutableNodeSchemaEntry toNodeSchemaEntry(Map.Entry<String, NodeSchema> entry) {
        var nodeLabel = NodeLabel.of(entry.getKey());
        var properties = toPropertySchemas(entry.getValue().propertySchemas());
        return new MutableNodeSchemaEntry(nodeLabel, properties); // TODO: Introduce factory
    }

    private static Map<String, PropertySchema> toPropertySchemas(Map<String, NodePropertySchema> schemas) {
        return schemas
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    CsvMetaDataFactory::toPropertySchema
                )
            );
    }

    private static PropertySchema toPropertySchema(Map.Entry<String, NodePropertySchema> schemaEntry) {
        var nodePropertySchema = schemaEntry.getValue();

        return PropertySchema.of(
            schemaEntry.getKey(),
            toValueType(nodePropertySchema.valueType()),
            toDefaultValue(nodePropertySchema.defaultValue()),
            toPropertyState(nodePropertySchema.propertyState())
        );
    }

    private static org.neo4j.gds.api.nodeproperties.ValueType toValueType(ValueType valueType) {
        return switch (valueType) {
            case LONG -> org.neo4j.gds.api.nodeproperties.ValueType.LONG;
            case DOUBLE -> org.neo4j.gds.api.nodeproperties.ValueType.DOUBLE;
            case FLOAT_ARRAY -> org.neo4j.gds.api.nodeproperties.ValueType.FLOAT_ARRAY;
            case DOUBLE_ARRAY -> org.neo4j.gds.api.nodeproperties.ValueType.DOUBLE_ARRAY;
            case LONG_ARRAY -> org.neo4j.gds.api.nodeproperties.ValueType.LONG_ARRAY;
        };
    }

    private static org.neo4j.gds.api.DefaultValue toDefaultValue(DefaultValue defaultValue) {
        return org.neo4j.gds.api.DefaultValue.of(defaultValue.defaultValue(), defaultValue.isUserDefined());
    }

    private static org.neo4j.gds.api.PropertyState toPropertyState(PropertyState propertyState) {
        return switch (propertyState) {
            case PERSISTENT -> org.neo4j.gds.api.PropertyState.PERSISTENT;
            case TRANSIENT -> org.neo4j.gds.api.PropertyState.TRANSIENT;
            case REMOTE -> org.neo4j.gds.api.PropertyState.REMOTE;
        };
    }

    private static Capabilities.WriteMode toWriteMode(GraphStoreMetadata graphStoreMetadata) {
        return switch(graphStoreMetadata.writeMode()) {
            case LOCAL -> Capabilities.WriteMode.LOCAL;
            case REMOTE -> Capabilities.WriteMode.REMOTE;
            case NONE -> Capabilities.WriteMode.NONE;
        };
    }

    private static Collection<? extends RelationshipType> toInverseRelationshipTypes(GraphStoreMetadata graphStoreMetadata) {
        return graphStoreMetadata
            .relationshipInfo().entrySet()
            .stream()
            .filter(entry -> entry.getValue().isInverseIndexed())
            .map(Map.Entry::getKey)
            .map(RelationshipType::of)
            .toList();
    }

    private static DatabaseInfo toDatabaseInfo(GraphStoreMetadata graphStoreMetadata) {
        var databaseId = toDatabaseId(graphStoreMetadata);
        var databaseLocation = toDatabaseLocation(graphStoreMetadata);
        var remoteDatabaseId = graphStoreMetadata
            .databaseInfo()
            .remoteDatabaseId()
            .map(DatabaseId::of);

        return ImmutableDatabaseInfo
            .builder()
            .databaseId(databaseId)
            .databaseLocation(databaseLocation)
            .remoteDatabaseId(remoteDatabaseId)
            .build();
    }

    private static DatabaseId toDatabaseId(GraphStoreMetadata graphStoreMetadata) {
        return DatabaseId.of(graphStoreMetadata.databaseInfo().databaseName());
    }

    private static DatabaseInfo.DatabaseLocation toDatabaseLocation(GraphStoreMetadata graphStoreMetadata) {
        return switch(graphStoreMetadata.databaseInfo().databaseLocation()) {
            case LOCAL -> DatabaseInfo.DatabaseLocation.LOCAL;
            case REMOTE -> DatabaseInfo.DatabaseLocation.REMOTE;
            case NONE -> DatabaseInfo.DatabaseLocation.NONE;
        };
    }

    private static Map<? extends RelationshipType, Long> toRelationshipTypeCounts(GraphStoreMetadata graphStoreMetadata) {
        return graphStoreMetadata
            .relationshipInfo().keySet()
            .stream()
            .collect(Collectors.toMap(
                RelationshipType::of,
                name -> graphStoreMetadata.relationshipInfo().get(name).relationshipCount()
            ));
    }
}
