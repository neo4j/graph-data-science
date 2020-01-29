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

public interface RelationshipIterator extends RelationshipPredicate {

    /**
     * Calls the given consumer function for every relationship of a given node.
     *
     * @param nodeId id of the node for which to iterate relationships
     * @param consumer relationship consumer function
     */
    void forEachRelationship(long nodeId, RelationshipConsumer consumer);

    /**
     * Calls the given consumer function for every relationship of a given node.
     * If the graph was loaded with a relationship property, the property value
     * of the relationship will be passed into the consumer. Otherwise the given
     * fallback value will be used.
     *
     * @param nodeId id of the node for which to iterate relationships
     * @param fallbackValue value used as relationship property if no properties were loaded
     * @param consumer relationship consumer function
     */
    void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer);

    /**
     * @return a copy of this iterator that reuses new cursors internally,
     *         so that iterations happen independent from other iterations.
     */
    default RelationshipIterator concurrentCopy() {
        return this;
    }

}
