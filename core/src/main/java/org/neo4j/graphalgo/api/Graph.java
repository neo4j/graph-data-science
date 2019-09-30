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

import org.neo4j.graphdb.Direction;

import java.util.Optional;

/**
 * Composition of often used source interfaces
 */
public interface Graph extends IdMapping, Degrees, NodeIterator, BatchNodeIterable, RelationshipIterator, RelationshipWeights, RelationshipAccess, NodeProperties {

    String TYPE = "huge";

    long RELATIONSHIP_COUNT_NOT_SUPPORTED = -1L;

    default boolean isEmpty() {
        return nodeCount() == 0;
    }

    /**
     * @return if supported, returns the total number of relationships in the graph, otherwise {@link Graph#RELATIONSHIP_COUNT_NOT_SUPPORTED}.
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

    default String getType() {
        return TYPE;
    }

    boolean isUndirected();

    boolean hasRelationshipProperty();

    Direction getLoadDirection();

    default Optional<Direction> compatibleDirection(Direction procedureDirection) {
        boolean isUndirected = this.isUndirected();
        Direction loadDirection = this.getLoadDirection();

        switch (procedureDirection) {
            case OUTGOING:
                if (!isUndirected && (loadDirection == Direction.BOTH || loadDirection == Direction.OUTGOING)) {
                    return Optional.of(Direction.OUTGOING);
                }
                break;
            case INCOMING:
                if (!isUndirected && (loadDirection == Direction.BOTH || loadDirection == Direction.INCOMING)) {
                    return Optional.of(Direction.INCOMING);
                }
                break;
            case BOTH:
                if (isUndirected && loadDirection == Direction.OUTGOING) {
                    return Optional.of(Direction.OUTGOING);
                }
                if (!isUndirected && loadDirection == Direction.BOTH) {
                    return Optional.of(Direction.BOTH);
                }
                break;
        }

        return Optional.empty();
    }

    void canRelease(boolean canRelease);

    RelationshipIntersect intersection();

}
