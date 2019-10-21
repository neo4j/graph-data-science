/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.api;

/**
 * Getter for property values at relationships
 */
public interface RelationshipProperties {

    /**
     * get value of property on relationship between source and target node id
     *
     * @param sourceNodeId source node
     * @param targetNodeId target node
     * @param fallbackValue value to use if relationship has no property value
     * @return the property value
     */
    double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue);

    /**
     * Returns the proeprty value for the relationship defined by their start and end nodes.
     */
    double relationshipProperty(long sourceNodeId, long targetNodeId);
}
