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

import org.neo4j.gds.ElementIdentifier;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.loading.Capabilities;

import java.util.Map;
import java.util.stream.Collectors;

public final class GraphStoreMetadataFactory {
    private GraphStoreMetadataFactory() {}

    public static GraphStoreMetadata fromGraphStore(GraphStore graphStore) {
        var databaseInfo = new DatabaseInfo(
            graphStore.databaseInfo().databaseId().databaseName(),
            toDatabaseLocation(graphStore.databaseInfo().databaseLocation()),
            graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
        );
        var writeMode = toWriteMode(graphStore.capabilities().writeMode());
        var idMapInfo = toIdMapInfo(graphStore.nodes());
        var relationshipInfo = toRelationshipInfo(graphStore);
        var nodeSchema = toNodeSchema(graphStore.schema().nodeSchema());
        var relationshipSchema = toRelationshipSchema(graphStore.schema().relationshipSchema());

        return new GraphStoreMetadata(
            databaseInfo,
            writeMode,
            idMapInfo,
            relationshipInfo,
            nodeSchema,
            relationshipSchema
        );
    }

    static DatabaseInfo.DatabaseLocation toDatabaseLocation(org.neo4j.gds.api.DatabaseInfo.DatabaseLocation databaseLocation) {
        return switch (databaseLocation) {
            case LOCAL -> DatabaseInfo.DatabaseLocation.LOCAL;
            case REMOTE -> DatabaseInfo.DatabaseLocation.REMOTE;
            case NONE -> DatabaseInfo.DatabaseLocation.NONE;
        };
    }

    static WriteMode toWriteMode(Capabilities.WriteMode writeMode) {
        return switch (writeMode) {
            case LOCAL -> WriteMode.LOCAL;
            case REMOTE -> WriteMode.REMOTE;
            case NONE -> WriteMode.NONE;
        };
    }

    static IdMapInfo toIdMapInfo(IdMap idMap) {
        return new IdMapInfo(
            idMap.typeId(),
            idMap.nodeCount(),
            idMap.highestOriginalId(),
            idMap.availableNodeLabels().stream()
                .collect(Collectors.toMap(
                        ElementIdentifier::name,
                        idMap::nodeCount
                    )
                )
        );
    }

    static Map<String, RelationshipInfo> toRelationshipInfo(GraphStore graphStore) {
        var inverseIndexedRelationshipTypes = graphStore.inverseIndexedRelationshipTypes();

        return graphStore.relationshipTypes().stream()
            .collect(Collectors.toMap(
                RelationshipType::name,
                relationshipType -> {
                    var relationshipCount = graphStore.relationshipCount(relationshipType);
                    var isInverseIndexed = inverseIndexedRelationshipTypes.contains(relationshipType);
                    var propertyCount = graphStore.relationshipPropertyKeys().size();

                    return new RelationshipInfo(relationshipCount, isInverseIndexed, propertyCount);
                }
            ));
    }

    static Map<String, NodeSchema> toNodeSchema(org.neo4j.gds.api.schema.NodeSchema nodeSchema) {
        return nodeSchema.availableLabels().stream().collect(Collectors.toMap(
            NodeLabel::name,
            nodeLabel -> {
                var propertySchemas = nodeSchema.propertySchemasFor(nodeLabel).stream().collect(Collectors.toMap(
                    PropertySchema::key,
                    propertySchema -> {
                        var valueType = toValueType(propertySchema.valueType());
                        var defaultValue = propertySchema.defaultValue().getObject();
                        var propertyState = toPropertyState(propertySchema.state());

                        return new NodePropertySchema(
                            valueType,
                            defaultValue,
                            propertyState
                        );

                    }
                ));

                return new NodeSchema(propertySchemas);
            }
        ));
    }

    static Map<String, RelationshipSchema> toRelationshipSchema(org.neo4j.gds.api.schema.RelationshipSchema relationshipSchema) {
        return relationshipSchema.availableTypes().stream().collect(Collectors.toMap(
            RelationshipType::name,
            relationshipType -> {
                var direction = toDirection(relationshipSchema, relationshipType);
                var propertySchemas = relationshipSchema.propertySchemasFor(relationshipType).stream().collect(
                    Collectors.toMap(
                        PropertySchema::key,
                        propertySchema -> {
                            var valueType = toValueType(propertySchema.valueType());
                            var defaultValue = propertySchema.defaultValue().getObject();
                            var propertyState = toPropertyState(propertySchema.state());
                            var aggregation = toAggregation(propertySchema.aggregation());

                            return new RelationshipPropertySchema(
                                valueType,
                                defaultValue,
                                propertyState,
                                aggregation
                            );

                        }
                    ));

                return new RelationshipSchema(direction, propertySchemas);
            }
        ));
    }

    static ValueType toValueType(org.neo4j.gds.api.nodeproperties.ValueType valueType) {
        return switch (valueType) {
            case LONG -> ValueType.LONG;
            case DOUBLE -> ValueType.DOUBLE;
            case DOUBLE_ARRAY -> ValueType.DOUBLE_ARRAY;
            case FLOAT_ARRAY -> ValueType.FLOAT_ARRAY;
            case LONG_ARRAY -> ValueType.LONG_ARRAY;
            case STRING -> throw new IllegalArgumentException(
                "String value type is not supported in graph store metadata");
            case UNTYPED_ARRAY -> throw new IllegalArgumentException(
                "Untyped array value type is not supported in graph store metadata");
            case UNKNOWN -> throw new IllegalArgumentException(
                "Unknown value type is not supported in graph store metadata");
        };
    }

    static PropertyState toPropertyState(org.neo4j.gds.api.PropertyState propertyState) {
        return switch (propertyState) {
            case PERSISTENT -> PropertyState.PERSISTENT;
            case TRANSIENT -> PropertyState.TRANSIENT;
            case REMOTE -> PropertyState.REMOTE;
        };
    }

    static Direction toDirection(
        org.neo4j.gds.api.schema.RelationshipSchema relationshipSchema,
        RelationshipType relationshipType
    ) {
        return relationshipSchema.isUndirected(relationshipType) ? Direction.UNDIRECTED : Direction.DIRECTED;
    }

    static Aggregation toAggregation(org.neo4j.gds.Aggregation aggregation) {
        return switch (aggregation) {
            case NONE -> Aggregation.NONE;
            case SINGLE -> Aggregation.SINGLE;
            case SUM -> Aggregation.SUM;
            case MIN -> Aggregation.MIN;
            case MAX -> Aggregation.MAX;
            case COUNT -> Aggregation.COUNT;
            case DEFAULT -> throw new IllegalArgumentException(
                "Default aggregation is not supported in graph store metadata");
        };
    }
}
