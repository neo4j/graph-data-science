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

/**
 * Cursor iterating over the values of relationship properties.
 * A lot of the methods here are very low-level and break when looked at slightly askew.
 * Better iteration methods and defined access patterns will be added under the continuation of
 * Adjacency Compression III – Return of the Iterator
 */
public interface PropertyCursor extends AutoCloseable {
    /**
     * Initialize this cursor to point to the given {@code index}.
     * The correct value for the index is highly implementation specific.
     * The better way to initialize a cursor is through {@link org.neo4j.graphalgo.api.AdjacencyProperties#propertyCursor(long)} or related.
     */
    void init(long index, int degree);

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

    /**
     * Returns a cursor that is always empty.
     */
    static PropertyCursor empty() {
        return EmptyPropertyCursor.INSTANCE;
    }

    enum EmptyPropertyCursor implements PropertyCursor {
        INSTANCE;

        @Override
        public void init(long index, int degree) {
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
