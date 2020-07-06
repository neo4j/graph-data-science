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

public interface AdjacencyCursor {

    // TODO: docs
    AdjacencyCursor init(long offset);

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
     * Return how many targets are still left to be decoded.
     */
    int remaining();

    /**
     * Read and decode target ids until it is strictly larger than ({@literal >}) the provided {@code target}.
     * Might return an id that is less than or equal to {@code target} iff the cursor did exhaust before finding an
     * id that is large enough.
     * {@code skipUntil(target) <= target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
     * will return {@code false}
     */
    long skipUntil(long target);

    /**
     * Read and decode target ids until it is larger than or equal ({@literal >=}) the provided {@code target}.
     * Might return an id that is less than {@code target} iff the cursor did exhaust before finding an
     * id that is large enough.
     * {@code advance(target) < target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
     * will return {@code false}
     */
    long advance(long target);
}
