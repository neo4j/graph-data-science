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

public interface CompositeRelationshipIterator {

    /**
     * Returns the degree of the given node id.
     */
    int degree(long nodeId);

    /**
     * Applies the given consumer on all relationships of the given node id.
     */
    void forEachRelationship(long nodeId, RelationshipConsumer consumer);

    /**
     * Calls the given consumer for every inverse relationship of a given node.
     * Inverse relationships basically mirror the relationships in the iterator.
     * For example, if `forEachRelationship(42)` returns `1337` then the
     * result of `forEachInverseRelationship(1337)` contains `42. For undirected
     * relationships, accessing the inverse is never supported.
     * <p>
     * Note, that the inverse index might not always be present. Check
     * {@link org.neo4j.gds.api.GraphStore#inverseIndexedRelationshipTypes()}
     * before calling this method to verify that the relevant relationship type is
     * inverse indexed.
     *
     * @param nodeId the node for which to iterate the inverse relationships
     * @param consumer relationship consumer function
     */
    void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer);

    /**
     * Returns the property keys that are managed by this iterator.
     * The order is equivalent to the order of the value array in
     * {@link CompositeRelationshipIterator.RelationshipConsumer#consume(long, long, double[])}.
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

