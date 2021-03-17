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

public interface CompositeRelationshipIterator {

    /**
     * Applies the given consumer on all relationships of the given node id.
     */
    void forEachRelationship(long nodeId, RelationshipConsumer consumer);

    /**
     * Returns the property keys that are managed by this iterator.
     * The order is equivalent to the order of the value array in
     * {@link org.neo4j.graphalgo.api.CompositeRelationshipIterator.RelationshipConsumer#consume(long, long, double[])}.
     */
    String[] propertyKeys();

    /**
     * Creates a thread-safe copy of the iterator.
     */
    CompositeRelationshipIterator concurrentCopy();

    @FunctionalInterface
    interface RelationshipConsumer {

        /**
         * This method is called for every relationship of the specified iterator.
         * The order of the {@code properties} is equivalent to the order of the property
         * key array returned by {@link CompositeRelationshipIterator#propertyKeys()}.
         * The provided array for {@code properties} might be reused across different invocations
         * and should not be stored without making a copy.
         */
        boolean consume(long source, long target, double[] properties);
    }
}

