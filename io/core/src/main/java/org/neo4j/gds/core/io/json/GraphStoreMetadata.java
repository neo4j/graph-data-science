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
    String adjacencyListType,
    long relationshipCount,
    boolean isInverseIndexed,
    int propertyCount
) {}

record NodeSchema(
    Map<String, NodePropertySchema> propertySchemas
) {}

record NodePropertySchema(
    ValueType valueType,
    Object defaultValue,
    PropertyState propertyState
) {}

record RelationshipSchema(
    Direction direction,
    Map<String, RelationshipPropertySchema> propertySchemas
) {}

record RelationshipPropertySchema(
    ValueType valueType,
    Object defaultValue,
    PropertyState propertyState,
    Aggregation aggregation
) {}

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
