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
package org.neo4j.gds.api.properties.relationships;

/**
 * TODO: Define interface contract, esp regarding source/target node ids returned and how that maps to relationship direction
 *
 * consumer interface for relationships without property.
 */
public interface RelationshipConsumer {

    /**
     * Called for every edge that matches a given relation-constraint
     *
     * @param sourceNodeId mapped source node id
     * @param targetNodeId mapped target node id
     * @return {@code true} if the iteration shall continue, otherwise {@code false}.
     */
    boolean accept(long sourceNodeId, long targetNodeId);

    default RelationshipConsumer andThen(RelationshipConsumer after) {
        return (sourceNodeId, targetNodeId) -> {
            this.accept(sourceNodeId, targetNodeId);
            return after.accept(sourceNodeId, targetNodeId);
        };
    }
}
