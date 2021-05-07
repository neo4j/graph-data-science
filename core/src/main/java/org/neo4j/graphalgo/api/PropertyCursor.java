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

public interface PropertyCursor extends AutoCloseable {
    /**
     * Initialize this cursor to point to the given {@code index}.
     */
    PropertyCursor init(long index, int degree);

    default boolean isEmpty() {
        return false;
    }

    /**
     * Return true iff there is at least one more target to decode.
     */
    boolean hasNextLong();

    /**
     * Read the next target id.
     *
     * It is undefined behavior if this is called after {@link #hasNextLong()} returns {@code false}.
     */
    long nextLong();

    @Override
    void close();

    static PropertyCursor empty() {
        return EmptyPropertyCursor.INSTANCE;
    }

    enum EmptyPropertyCursor implements PropertyCursor {
        INSTANCE;

        @Override
        public PropertyCursor init(long index, int degree) {
            return INSTANCE;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean hasNextLong() {
            return false;
        }

        @Override
        public long nextLong() {
            return -1;
        }

        @Override
        public void close() {
        }
    }
}
