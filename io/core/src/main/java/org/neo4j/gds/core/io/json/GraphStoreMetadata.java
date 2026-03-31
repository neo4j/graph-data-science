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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;
import java.util.Optional;

public record GraphStoreMetadata(
    DatabaseInfo databaseInfo,
    WriteMode writeMode,
    IdMapInfo idMapInfo,
    Map<String, RelationshipInfo> relationshipInfo,
    Map<String, NodeSchema> nodeSchema,
    Map<String, RelationshipSchema> relationshipSchema
) {
}

record DatabaseInfo(
    String databaseName,
    DatabaseLocation databaseLocation,
    Optional<String> remoteDatabaseId
) {
    enum DatabaseLocation {
        LOCAL, REMOTE, NONE
    }
}

record IdMapInfo(
    String idMapType,
    long nodeCount,
    long maxOriginalId,
    Map<String, Long> nodeLabelCounts
) {
}

record RelationshipInfo(
    long relationshipCount,
    boolean isInverseIndexed,
    int propertyCount
) {}

record NodeSchema(
    Map<String, NodePropertySchema> propertySchemas
) {}

record NodePropertySchema(
    ValueType valueType,
    DefaultValue defaultValue,
    PropertyState propertyState
) {}

record RelationshipSchema(
    Direction direction,
    Map<String, RelationshipPropertySchema> propertySchemas
) {}

record RelationshipPropertySchema(
    ValueType valueType,
    DefaultValue defaultValue,
    PropertyState propertyState,
    Aggregation aggregation
) {}

record DefaultValue(
    @JsonProperty("value")
    @JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.WRAPPER_OBJECT
    )
    @JsonSubTypes({
        @JsonSubTypes.Type(value = Long.class, name = "long"),
        @JsonSubTypes.Type(value = Double.class, name = "double"),
        @JsonSubTypes.Type(value = long[].class, name = "long_array"),
        @JsonSubTypes.Type(value = double[].class, name = "double_array"),
        @JsonSubTypes.Type(value = float[].class, name = "float_array")
    })
    Object defaultValue,
    boolean isUserDefined
) {
    @JsonCreator
    public static DefaultValue of(
        @JsonProperty("value") Object defaultValue,
        @JsonProperty("isUserDefined") boolean isUserDefined
    ) {
        return new DefaultValue(defaultValue, isUserDefined);
    }
}

enum Aggregation {
    NONE, SINGLE, SUM, MIN, MAX, COUNT
}

enum Direction {
    DIRECTED, UNDIRECTED
}

enum PropertyState {
   PERSISTENT, TRANSIENT, REMOTE
}

enum ValueType {
    LONG, DOUBLE, FLOAT_ARRAY, DOUBLE_ARRAY, LONG_ARRAY
}

enum WriteMode {
    LOCAL, REMOTE, NONE
}
