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
package org.neo4j.graphalgo.api;

/**
 * Composition of often used source interfaces
 */
public interface Graph extends IdMapping, Degrees, NodeIterator, BatchNodeIterable, RelationshipIterator, RelationshipProperties, RelationshipAccess, NodePropertyContainer {

    default boolean isEmpty() {
        return nodeCount() == 0;
    }

    /**
     * @return returns the total number of relationships in the graph.
     */
    long relationshipCount();

    /**
     * Release all resources which are not part of the result or IdMapping
     */
    default void release() {
        releaseTopology();
        releaseProperties();
    }

    /**
     * Release only the topological data associated with that graph.
     */
    default void releaseTopology() { }

    /**
     * Release only the properties associated with that graph.
     */
    default void releaseProperties() { }

    boolean isUndirected();

    boolean hasRelationshipProperty();

    void canRelease(boolean canRelease);

    RelationshipIntersect intersection();
}
