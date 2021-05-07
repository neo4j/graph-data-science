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

public interface AdjacencyCursor extends AutoCloseable {

    long NOT_FOUND = -1;

    /**
     * Initialize this cursor to point to the given {@code index}.
     */
    void init(long index, int degree);

    /**
     * Initialize this cursor to point to the given {@code index}.
     * Returns this to allow chaining.
     */
    default AdjacencyCursor initializedTo(long index, int degree) {
        this.init(index, degree);
        return this;
    }

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

    default boolean isEmpty() {
        return remaining() == 0;
    }

    // DOCTODO: I think this documentation if either out of date or misleading.
    //  Either we skip all blocks and return -1 or we find a value that is strictly larger.
    /**
     * Read and decode target ids until it is strictly larger than ({@literal >}) the provided {@code target}.
     * Might return an id that is less than or equal to {@code target} iff the cursor did exhaust before finding an
     * id that is large enough.
     * {@code skipUntil(target) <= target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
     * will return {@code false}
     */
    long skipUntil(long nodeId);

    /**
     * Read and decode target ids until it is larger than or equal ({@literal >=}) the provided {@code target}.
     * Might return an id that is less than {@code target} iff the cursor did exhaust before finding an
     * id that is large enough.
     * {@code advance(target) < target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
     * will return {@code false}
     */
    long advance(long nodeId);

    /**
     * Copies internal states from {@code sourceCursor} into {@code this} cursor.
     * If the types don't match, the behavior is undefined.
     */
    void copyFrom(AdjacencyCursor sourceCursor);

    @Override
    void close();

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
        public boolean isEmpty() {
            return true;
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
        public void copyFrom(AdjacencyCursor sourceCursor) {
        }

        @Override
        public void close() {
        }
    }
}
