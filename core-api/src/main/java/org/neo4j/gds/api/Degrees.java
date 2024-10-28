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

/**
 * The Degree interface is intended to return the degree of a given node.
 */
public interface Degrees {

    /**
     * Returns the number of relationships connected to that node.
     * For undirected graphs, this includes outgoing and incoming
     * relationships. For directed graphs, this is the number of
     * outgoing edges.
     */
    int degree(long nodeId);

    /**
     * Returns the number of relationships connected to that node.
     * For directed graphs, this is the number of incoming edges.
     * For undirected graphs, the behaviour of this method is undefined.
     *
     * Note, that this is an optional feature, and it is up to the implementation
     * if this is actually supported. Check {@link org.neo4j.gds.api.Graph#characteristics()}
     * before calling this method to verify that the graphs is inverse indexed.
     */
    int degreeInverse(long nodeId);

    /**
     * Much slower than just degree() because it may have to look up all relationships.
     * <br>
     * This is not thread-safe, so if this is called concurrently please use {@link RelationshipIterator#concurrentCopy()}.
     *
     * @see Graph#isMultiGraph()
     */
    int degreeWithoutParallelRelationships(long nodeId);
}
