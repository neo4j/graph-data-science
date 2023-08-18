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
package org.neo4j.gds.applications.graphstorecatalog;

public class GraphStreamRelationshipPropertiesResult {
    public final long sourceNodeId;
    public final long targetNodeId;
    public final String relationshipType;
    public final String relationshipProperty;
    public final Number propertyValue;

    public GraphStreamRelationshipPropertiesResult(
        long sourceNodeId,
        long targetNodeId,
        String relationshipType,
        String relationshipProperty,
        Number propertyValue
    ) {
        this.sourceNodeId = sourceNodeId;
        this.targetNodeId = targetNodeId;
        this.relationshipType = relationshipType;
        this.relationshipProperty = relationshipProperty;
        this.propertyValue = propertyValue;
    }
}
