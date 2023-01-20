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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Cursor iterating over the target ids of one adjacency list.
 * A lot of the methods here are very low-level and break when looked at slightly askew.
 * Better iteration methods and defined access patterns will be added under the continuation of
 * Adjacency Compression III – Return of the Iterator
 */
public interface AdjacencyCursor {

    /**
     * Special ID value that could be returned to indicate that no valid value can be produced
     */
    long NOT_FOUND = -1;

    /**
     * Initialize this cursor to point to the given {@code index}.
     * The correct value for the index is highly implementation specific.
     * The better way get initialize a cursor is through {@link AdjacencyList#adjacencyCursor(long)} or related.
     */
    void init(long index, int degree);

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
     *
     * It is undefined behavior if this is called after {@link #hasNextVLong()} returns {@code false}.
     */
    long peekVLong();

    /**
     * Return how many targets are still left to be decoded.
     */
    int remaining();

    /**
     * Read and decode target ids until it is strictly larger than ({@literal >}) the provided {@code target}.
     * If there are no such targets before this cursor is exhausted, {@link org.neo4j.gds.api.AdjacencyCursor#NOT_FOUND -1} is returned.
     */
    long skipUntil(long nodeId);

    /**
     * Read and decode target ids until it is larger than or equal ({@literal >=}) the provided {@code target}.
     * If there are no such targets before this cursor is exhausted, {@link org.neo4j.gds.api.AdjacencyCursor#NOT_FOUND -1} is returned.
     */
    long advance(long nodeId);

    /**
     * Advance this cursor by {@code n} elements.
     * For a cursor in its initial position, this is equivalent to {@code nth}.
     *
     * @param n the number of elements to advance by. Must be positive.
     * @return the target after the advancement or {@link org.neo4j.gds.api.AdjacencyCursor#NOT_FOUND -1} if the cursor is exhausted.
     */
    long advanceBy(int n);

    /**
     * Create a shallow copy of this cursor.
     * Iteration state is copied and will advance independently from this cursor.
     * The underlying data might be shared between instances.
     * If the provided {@code destination} argument is not null, it might be re-used instead of having to create a new instance.
     * It is *not* guaranteed that the {@code destination} will be reused.
     * If the {@code destination} is not if the same type than this cursor, the behavior of this method in undefined.
     */
    @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination);

    /**
     * Returns a cursor that is always empty.
     */
    static AdjacencyCursor empty() {
        return EmptyAdjacencyCursor.INSTANCE;
    }

    enum EmptyAdjacencyCursor implements AdjacencyCursor {
        INSTANCE;

        @Override
        public void init(long index, int degree) {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean hasNextVLong() {
            return false;
        }

        @Override
        public long nextVLong() {
            return NOT_FOUND;
        }

        @Override
        public long peekVLong() {
            return NOT_FOUND;
        }

        @Override
        public int remaining() {
            return 0;
        }

        @Override
        public long skipUntil(long nodeId) {
            return NOT_FOUND;
        }

        @Override
        public long advance(long nodeId) {
            return NOT_FOUND;
        }

        @Override
        public long advanceBy(int n) {
            return NOT_FOUND;
        }

        @Override
        public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
            return INSTANCE;
        }
    }
}
