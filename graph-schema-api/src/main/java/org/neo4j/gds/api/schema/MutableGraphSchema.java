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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ValueClass
public
interface MutableGraphSchema extends GraphSchema {
    @Override
    MutableNodeSchema nodeSchema();

    @Override
    MutableRelationshipSchema relationshipSchema();


    @Override
    default MutableGraphSchema filterNodeLabels(Set<NodeLabel> labelsToKeep) {
        return of(nodeSchema().filter(labelsToKeep), relationshipSchema(), graphProperties());
    }

    @Override
    default MutableGraphSchema filterRelationshipTypes(Set<RelationshipType> relationshipTypesToKeep) {
        return of(nodeSchema(), relationshipSchema().filter(relationshipTypesToKeep), graphProperties());
    }

    @Override
    default MutableGraphSchema union(GraphSchema other) {
        return MutableGraphSchema.of(
            nodeSchema().union(other.nodeSchema()),
            relationshipSchema().union(other.relationshipSchema()),
            unionGraphProperties(other.graphProperties())
        );
    }

    static MutableGraphSchema empty() {
        return of(MutableNodeSchema.empty(), MutableRelationshipSchema.empty(), Map.of());
    }

    static MutableGraphSchema from(GraphSchema from) {
        return of(
            MutableNodeSchema.from(from.nodeSchema()),
            MutableRelationshipSchema.from(from.relationshipSchema()),
            new HashMap<>(from.graphProperties())
        );
    }

    static MutableGraphSchema of(
        MutableNodeSchema nodeSchema,
        MutableRelationshipSchema relationshipSchema,
        Map<String, PropertySchema> graphProperties
    ) {
        return ImmutableMutableGraphSchema.builder()
            .nodeSchema(nodeSchema)
            .relationshipSchema(relationshipSchema)
            .graphProperties(graphProperties)
            .build();
    }

    static ImmutableMutableGraphSchema.Builder builder() {
        return ImmutableMutableGraphSchema.builder();
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
}
