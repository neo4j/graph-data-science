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
import org.neo4j.gds.annotation.ValueClass;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ValueClass
public interface GraphSchema {

    NodeSchema nodeSchema();

    RelationshipSchema relationshipSchema();

    Map<String, PropertySchema> graphProperties();

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

    default GraphSchema filterNodeLabels(Set<NodeLabel> labelsToKeep) {
        return of(nodeSchema().filter(labelsToKeep), relationshipSchema(), graphProperties());
    }

    default GraphSchema filterRelationshipTypes(Set<RelationshipType> relationshipTypesToKeep) {
        return of(nodeSchema(), relationshipSchema().filter(relationshipTypesToKeep), graphProperties());
    }

    default GraphSchema union(GraphSchema other) {
        return GraphSchema.of(
            nodeSchema().union(other.nodeSchema()),
            relationshipSchema().union(other.relationshipSchema()),
            unionGraphProperties(other.graphProperties())
        );
    }

    private Map<String, PropertySchema> unionGraphProperties(Map<String, PropertySchema> otherProperties) {
        return Stream.concat(
            graphProperties().entrySet().stream(),
            otherProperties.entrySet().stream()
        ).collect(Collectors.toMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (leftType, rightType) -> {
                if (leftType.valueType() != rightType.valueType()) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Combining schema entries with value type %s and %s is not supported.",
                        leftType.valueType(),
                        rightType.valueType()
                    ));
                } else {
                    return leftType;
                }
            }
        ));
    }

    static GraphSchema of(NodeSchema nodeSchema, RelationshipSchema relationshipSchema, Map<String, PropertySchema> graphProperties) {
        return ImmutableGraphSchema.builder()
            .nodeSchema(nodeSchema)
            .relationshipSchema(relationshipSchema)
            .graphProperties(graphProperties)
            .build();
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

    static GraphSchema empty() {
        return of(NodeSchema.empty(), RelationshipSchema.empty(), Map.of());
    }

    default boolean isUndirected() {
        return relationshipSchema().isUndirected();
    }

}
