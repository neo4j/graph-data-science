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
package org.neo4j.gds.collections.cursor;

public interface HugeCursorSupport<Array> {

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    long size();

    /**
     * Returns a new {@link HugeCursor} for this array. The cursor is not positioned and in an invalid state.
     *
     * To position the cursor you must call {@link #initCursor(HugeCursor)} or {@link #initCursor(HugeCursor, long, long)}.
     * Then the cursor needs to be put in a valid state by calling {@link HugeCursor#next()}.
     *
     * Obtaining a {@link HugeCursor} for an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    HugeCursor<Array> newCursor();

    /**
     * Resets the {@link HugeCursor} to range from index 0 until {@link #size()}.
     *
     * The returned cursor is not positioned and in an invalid state.
     * You must call {@link HugeCursor#next()} first to position the cursor to a valid state.
     *
     * The returned cursor is the reference-same ({@code ==}) one as the provided one.
     *
     * Resetting the {@link HugeCursor} of an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    default HugeCursor<Array> initCursor(HugeCursor<Array> cursor) {
        cursor.setRange();
        return cursor;
    }

    /**
     * Resets the {@link HugeCursor} to range from index {@code start} (inclusive, the first index to be contained)
     * until {@code end} (exclusive, the first index not to be contained).
     *
     * The returned cursor is not positioned and in an invalid state.
     * You must call {@link HugeCursor#next()} first to position the cursor to a valid state.
     *
     * The returned cursor is the reference-same ({@code ==}) one as the provided one.
     *
     * Resetting the {@link HugeCursor} of an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     *
     * @see org.neo4j.gds.collections.haa.HugeAtomicLongArray#initCursor(HugeCursor)
     */
    default HugeCursor<Array> initCursor(HugeCursor<Array> cursor, long start, long end) {
        if (start < 0L || start > size()) {
            throw new IllegalArgumentException("start expected to be in [0 : " + size() + "] but got " + start);
        }
        if (end < start || end > size()) {
            throw new IllegalArgumentException("end expected to be in [" + start + " : " + size() + "] but got " + end);
        }
        cursor.setRange(start, end);
        return cursor;
    }
}
