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

import java.util.Set;

@ValueClass
@SuppressWarnings({"immutables:subtype", "immutables:from"})
public interface MutableGraphSchema extends GraphSchema {
    @Override
    MutableNodeSchema nodeSchema();

    @Override
    MutableRelationshipSchema relationshipSchema();

    @Override
    default MutableGraphSchema filterNodeLabels(Set<NodeLabel> labelsToKeep) {
        return of(nodeSchema().filter(labelsToKeep), relationshipSchema());
    }

    @Override
    default MutableGraphSchema filterRelationshipTypes(Set<RelationshipType> relationshipTypesToKeep) {
        return of(nodeSchema(), relationshipSchema().filter(relationshipTypesToKeep));
    }

    @Override
    default MutableGraphSchema union(GraphSchema other) {
        return MutableGraphSchema.of(
            nodeSchema().union(other.nodeSchema()),
            relationshipSchema().union(other.relationshipSchema())
        );
    }

    static MutableGraphSchema empty() {
        return of(MutableNodeSchema.empty(), MutableRelationshipSchema.empty());
    }

    static MutableGraphSchema from(GraphSchema from) {
        return of(
            MutableNodeSchema.from(from.nodeSchema()),
            MutableRelationshipSchema.from(from.relationshipSchema())
        );
    }

    static MutableGraphSchema of(
        MutableNodeSchema nodeSchema,
        MutableRelationshipSchema relationshipSchema
    ) {
        return ImmutableMutableGraphSchema.builder()
            .nodeSchema(nodeSchema)
            .relationshipSchema(relationshipSchema)
            .build();
    }

    static ImmutableMutableGraphSchema.Builder builder() {
        return ImmutableMutableGraphSchema.builder();
    }

    static ImmutableMutableGraphSchema.Builder builderFrom(GraphSchema parent) {
        return builder()
            .nodeSchema(MutableNodeSchema.from(parent.nodeSchema()))
            .relationshipSchema(MutableRelationshipSchema.from(parent.relationshipSchema()));
    }
}
