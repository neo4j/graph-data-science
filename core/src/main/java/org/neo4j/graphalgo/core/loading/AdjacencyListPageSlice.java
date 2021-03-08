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
package org.neo4j.graphalgo.core.loading;

public interface AdjacencyListPageSlice {

    /**
     * Global address of the {@link #offset()} in this {@link #page()}.
     */
    long address();

    /**
     * Write some bytes at the current {@link #offset()}.
     */
    default void insert(byte[] bytes, int arrayOffset, int length) {
        System.arraycopy(bytes, arrayOffset, page(), offset(), length);
        bytesWritten(length);
    }

    /**
     * The current page. Only writes starting at {@link #offset} are safe.
     */
    byte[] page();

    /**
     * Start offset for safe writes into the {@link #page}.
     */
    int offset();

    /**
     * Notify that this many bytes have been written to the {@link #page}.
     */
    void bytesWritten(int numberOfBytes);
}
