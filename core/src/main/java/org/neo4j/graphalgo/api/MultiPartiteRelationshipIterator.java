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

import org.neo4j.graphalgo.RelationshipType;

import java.util.Set;
import java.util.stream.Stream;

public interface MultiPartiteRelationshipIterator extends RelationshipIterator {

    void forEachRelationship(long nodeId, Set<RelationshipType> relationshipTypes, RelationshipConsumer consumer);

    void forEachRelationship(
        long nodeId,
        double fallbackValue,
        Set<RelationshipType> relationshipTypes,
        RelationshipWithPropertyConsumer consumer
    );

    Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue, Set<RelationshipType> relationshipTypes);

    @Override
    default void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        forEachRelationship(nodeId, Set.of(), consumer);
    }

    @Override
    default void forEachRelationship(
        long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer
    ) {
        forEachRelationship(nodeId, fallbackValue, Set.of(), consumer);
    }

    @Override
    default Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        return streamRelationships(nodeId, fallbackValue, Set.of());
    }

    @Override
    default boolean exists(long sourceNodeId, long targetNodeId) {
        return false;
    }
}
