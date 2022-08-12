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
package org.neo4j.gds.api;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.properties.nodes.NodePropertyContainer;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.huge.NodeFilteredGraph;

import java.util.Optional;
import java.util.Set;

public interface Graph extends IdMap, NodePropertyContainer, Degrees, RelationshipIterator, RelationshipProperties {

    GraphSchema schema();

    default boolean isEmpty() {
        return nodeCount() == 0;
    }

    /**
     * @return returns the total number of relationships in the graph.
     */
    long relationshipCount();

    /**
     * Release all resources which are not part of the result or IdMap
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

    /**
     * Whether the graph is guaranteed to have no parallel relationships.
     * If this returns {@code false} it still may be parallel-free, but we do not know.
     * @return {@code true} iff the graph has maximum one relationship between each pair of nodes.
     */
    boolean isMultiGraph();

    Graph relationshipTypeFilteredGraph(Set<RelationshipType> relationshipTypes);

    boolean hasRelationshipProperty();

    void canRelease(boolean canRelease);

    @Override
    Graph concurrentCopy();

    /**
     * If this graph is created using a node label filter, this will return a NodeFilteredGraph that represents the node set used in this graph.
     * Be aware that it is not guaranteed to contain all relationships of the graph.
     * Otherwise, it will return an empty Optional.
     */
    Optional<NodeFilteredGraph> asNodeFilteredGraph();
}
