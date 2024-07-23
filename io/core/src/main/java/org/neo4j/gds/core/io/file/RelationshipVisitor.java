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
package org.neo4j.gds.core.io.file;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.schema.RelationshipPropertySchema;
import org.neo4j.gds.api.schema.RelationshipSchema;
import org.neo4j.gds.compat.batchimport.input.Group;
import org.neo4j.gds.core.io.IdentifierMapper;

import java.util.List;

public abstract class RelationshipVisitor extends ElementVisitor<RelationshipPropertySchema> {

    private final RelationshipSchema relationshipSchema;
    private final IdentifierMapper<RelationshipType> relationshipTypeMapping;

    private long currentStartNode;
    private long currentEndNode;
    private String relationshipType;

    protected RelationshipVisitor(
        RelationshipSchema relationshipSchema,
        IdentifierMapper<RelationshipType> relationshipTypeMapping
    ) {
        super(relationshipSchema.allProperties());
        this.relationshipSchema = relationshipSchema;
        this.relationshipTypeMapping = relationshipTypeMapping;
        reset();
    }

    // Accessors for relationship related data

    public long startNode() {
        return currentStartNode;
    }

    public long endNode() {
        return currentEndNode;
    }

    public String relationshipType() {
        return relationshipTypeMapping.identifierFor(RelationshipType.of(relationshipType));
    }

    // Additional listeners for relationship related data

    @Override
    public boolean startId(long id) {
        currentStartNode = id;
        return true;
    }

    @Override
    public boolean startId(long id, Group group) {
        return startId(id);
    }

    @Override
    public boolean endId(long id) {
        currentEndNode = id;
        return true;
    }

    @Override
    public boolean endId(long id, Group group) {
        return endId(id);
    }

    @Override
    public boolean type(String type) {
        relationshipType = type;
        return true;
    }

    // Overrides from ElementVisitor

    @Override
    String elementIdentifier() {
        return relationshipType;
    }

    @Override
    protected List<RelationshipPropertySchema> getPropertySchema() {
        return relationshipSchema.propertySchemasFor(RelationshipType.of(relationshipType));
    }

    @Override
    public void reset() {
        currentStartNode = -1;
        currentEndNode = -1;
    }

    protected abstract static class Builder<SELF extends RelationshipVisitor.Builder<SELF, VISITOR>, VISITOR extends RelationshipVisitor> {
        protected RelationshipSchema relationshipSchema;

        public SELF withRelationshipSchema(RelationshipSchema relationshipSchema) {
            this.relationshipSchema = relationshipSchema;
            return me();
        }

        public abstract SELF me();

        protected abstract VISITOR build();
    }
}
