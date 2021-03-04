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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.schema.GraphSchema;

import java.util.Set;

public interface Graph extends NodeMapping, NodePropertyContainer, Degrees, RelationshipIterator, RelationshipProperties, RelationshipMapping, RelationshipAccess {

    GraphSchema schema();

    NodeMapping nodeMapping();

    @Override
    default Set<NodeLabel> nodeLabels(long nodeId) {
        return nodeMapping().nodeLabels(nodeId);
    }

    @Override
    default Set<NodeLabel> availableNodeLabels() {
        return nodeMapping().availableNodeLabels();
    }

    @Override
    default Set<RelationshipType> availableRelationshipTypes() {
        return schema().relationshipSchema().properties().keySet();
    }

    default boolean isEmpty() {
        return nodeMapping().nodeCount() == 0;
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

    /**
     * Whether the graph is guaranteed to have no parallel relationships.
     * If this returns {@code false} it still may be parallel-free, but we do not know.
     * @return {@code true} iff the graph has maximum one relationship between each pair of nodes.
     */
    boolean isMultiGraph();

    boolean hasRelationshipProperty();

    void canRelease(boolean canRelease);

    @Override
    Graph concurrentCopy();
}
