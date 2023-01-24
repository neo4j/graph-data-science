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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface GraphSchema {

    NodeSchema nodeSchema();

    RelationshipSchema relationshipSchema();

    Map<String, PropertySchema> graphProperties();

    GraphSchema filterNodeLabels(Set<NodeLabel> labelsToKeep);

    GraphSchema filterRelationshipTypes(Set<RelationshipType> relationshipTypesToKeep);

    GraphSchema union(GraphSchema other);

    default Map<String, Object> toMap() {
        return Map.of(
            "nodes", nodeSchema().toMap(),
            "relationships", relationshipSchema().toMap(),
            "graphProperties", graphProperties().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                schema -> GraphSchema.forPropertySchema(schema.getValue())
            ))
        );
    }

    default Map<String, Object> toMapOld() {
        return Map.of(
            "nodes", nodeSchema().toMap(),
            "relationships", relationshipSchema().toMapOld(),
            "graphProperties", graphProperties().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                schema -> GraphSchema.forPropertySchema(schema.getValue())
            ))
        );
    }

    default boolean isUndirected() {
        return relationshipSchema().isUndirected();
    }

    default Direction direction() {
        return relationshipSchema().isUndirected() ? Direction.UNDIRECTED : Direction.DIRECTED;
    }

    static <PS extends PropertySchema> String forPropertySchema(PS propertySchema) {
        if (propertySchema instanceof RelationshipPropertySchema) {
            return String.format(
                Locale.ENGLISH,
                "%s (%s, %s, Aggregation.%s)",
                propertySchema.valueType().cypherName(),
                propertySchema.defaultValue(),
                propertySchema.state(),
                ((RelationshipPropertySchema) propertySchema).aggregation());
        }
        return String.format(
            Locale.ENGLISH,
            "%s (%s, %s)",
            propertySchema.valueType().cypherName(),
            propertySchema.defaultValue(),
            propertySchema.state()
        );
    }

}
