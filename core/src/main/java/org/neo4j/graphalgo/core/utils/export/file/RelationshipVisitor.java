/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export.file;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.schema.RelationshipPropertySchema;
import org.neo4j.graphalgo.api.schema.RelationshipSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class RelationshipVisitor extends ElementVisitor<RelationshipSchema, RelationshipType, RelationshipPropertySchema> {

    private long currentStartNode;
    private long currentEndNode;
    private String relationshipType;

    protected RelationshipVisitor(RelationshipSchema relationshipSchema) {
        super(relationshipSchema);
        reset();
    }

    // Accessors for node related data

    public long startNode() {
        return currentStartNode;
    }

    public long endNode() {
        return currentEndNode;
    }

    public String relationshipType() {
        return relationshipType;
    }

    // Additional listeners for node related data

    @Override
    public boolean startId(long id) {
        currentStartNode = id;
        return true;
    }

    @Override
    public boolean endId(long id) {
        currentEndNode = id;
        return true;
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
    List<RelationshipPropertySchema> getPropertySchema() {
        var relationshipType = RelationshipType.of(this.relationshipType);
        var propertySchemaForLabels = elementSchema.filter(Set.of(relationshipType));
        return new ArrayList<>(propertySchemaForLabels.unionProperties().values());
    }

    @Override
    void reset() {
        currentStartNode = -1;
        currentEndNode = -1;
    }
}
