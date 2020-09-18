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

public interface AdjacencyCursor extends AutoCloseable {

    long NOT_FOUND = -1;

    /**
     * Return how many targets can be decoded in total. This is equivalent to the degree.
     */
    int size();

    /**
     * Return true iff there is at least one more target to decode.
     */
    boolean hasNextVLong();

    /**
     * Read and decode the next target id.
     *
     * It is undefined behavior if this is called after {@link #hasNextVLong()} returns {@code false}.
     */
    long nextVLong();

    /**
     * Decode and peek the next target id. Does not progress the internal cursor unlike {@link #nextVLong()}.
     */
    long peekVLong();

    /**
     * Return how many targets are still left to be decoded.
     */
    int remaining();

    @Override
    void close();
}
