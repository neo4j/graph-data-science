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

/**
 * @author mknblch
 */
public interface HugeRelationshipIterator {

    void forEachRelationship(
            long nodeId,
            Direction direction,
            HugeRelationshipConsumer consumer);

    void forEachRelationship(
            long nodeId,
            Direction direction,
            HugeWeightedRelationshipConsumer consumer);

    default void forEachIncoming(
            long nodeId,
            HugeRelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.INCOMING, consumer);
    }

    default void forEachOutgoing(
            long nodeId,
            HugeRelationshipConsumer consumer) {
        forEachRelationship(nodeId, Direction.OUTGOING, consumer);
    }

    /**
     * @return a copy of this iterator that reuses new cursors internally,
     *         so that iterations happen independent from other iterations.
     */
    default HugeRelationshipIterator concurrentCopy() {
        return this;
    }

    /**
     * @return a copy of this iterator that is capable of intersecting two
     *         adjacency lists independent from other iterations.
     */
    /*
    default RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("Not implemented");
    }
    */
}
