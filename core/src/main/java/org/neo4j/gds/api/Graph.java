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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.properties.nodes.NodePropertyContainer;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.huge.NodeFilteredGraph;

import java.util.Optional;
import java.util.Set;

@SuppressFBWarnings("UCF_USELESS_CONTROL_FLOW")
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

    boolean isUndirected();

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

    /**
     * Get the n-th target node id for a given {@code sourceNodeId}.
     *
     * The order of the targets is not defined and depends on the implementation of the graph,
     * but it is consistent across separate calls to this method on the same graph.
     *
     * The {@code sourceNodeId} must be a node id existing in the graph.
     * The {@code offset} parameter is 0-indexed and must be positive.
     * If {@code offset} is greater than the number of targets for {@code sourceNodeId}, {@link IdMap#NOT_FOUND -1} is returned.
     *
     * It is undefined behavior if the {@code sourceNodeId} does not exist in the graph or the {@code offset} is negative.
     *
     * @param offset the {@code n}-th target to return. Must be positive.
     * @return the target at the {@code offset} or {@link IdMap#NOT_FOUND -1} if there is no such target.
     */
    default long nthTarget(long sourceNodeId, int offset) {
        return Graph.nthTarget(this, sourceNodeId, offset);
    }

    /**
     * see {@link #nthTarget(long, int)}
     */
    static long nthTarget(Graph graph, long sourceNodeId, int offset) {
        class FindNth implements RelationshipConsumer {
            private int remaining = offset;
            private long target = NOT_FOUND;

            @Override
            public boolean accept(long sourceNodeId, long targetNodeId) {
                if (remaining-- == 0) {
                    target = targetNodeId;
                    return false;
                }
                return true;
            }
        }

        if (offset >= graph.degree(sourceNodeId)) {
            return NOT_FOUND;
        }

        assert offset >= 0 : "offset must be positive, got " + offset;

        var findN = new FindNth();
        graph.forEachRelationship(sourceNodeId, findN);
        return findN.target;
    }
}
