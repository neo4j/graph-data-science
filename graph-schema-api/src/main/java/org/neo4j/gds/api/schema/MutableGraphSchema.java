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
import org.neo4j.gds.annotation.GenerateBuilder;

import java.util.Set;

@GenerateBuilder
public record MutableGraphSchema(NodeSchema nodeSchema, MutableRelationshipSchema relationshipSchema) implements GraphSchema {

    public MutableGraphSchema filterNodeLabels(Set<NodeLabel> labelsToKeep) {
        return of(nodeSchema().filter(labelsToKeep), relationshipSchema());
    }

    public MutableGraphSchema filterRelationshipTypes(Set<RelationshipType> relationshipTypesToKeep) {
        return of(nodeSchema(), relationshipSchema().filter(relationshipTypesToKeep));
    }

    public MutableGraphSchema union(GraphSchema other) {
        return MutableGraphSchema.of(
            nodeSchema().union(other.nodeSchema()),
            relationshipSchema().union(other.relationshipSchema())
        );
    }

    public static MutableGraphSchema empty() {
        return of(NodeSchema.empty(), MutableRelationshipSchema.empty());
    }

    public static MutableGraphSchema from(GraphSchema from) {
        return of(
            from.nodeSchema(),
            MutableRelationshipSchema.from(from.relationshipSchema())
        );
    }

    public static MutableGraphSchema of(NodeSchema nodeSchema, MutableRelationshipSchema relationshipSchema) {
        return new MutableGraphSchema(nodeSchema, relationshipSchema);
    }

    public static MutableGraphSchemaBuilder builder() {
        return MutableGraphSchemaBuilder.builder();
    }

    public static MutableGraphSchemaBuilder builderFrom(GraphSchema parent) {
        return builder()
            .nodeSchema(parent.nodeSchema())
            .relationshipSchema(MutableRelationshipSchema.from(parent.relationshipSchema()));
    }
}
