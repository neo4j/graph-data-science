/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.paged;

import java.util.Arrays;
import java.util.function.LongFunction;

abstract class HugeArray<Array, Box, Self extends HugeArray<Array, Box, Self>> {

    /**
     * Copies the content of this array into the target array.
     * <p>
     * The behavior is identical to {@link System#arraycopy(Object, int, Object, int, int)}.
     */
    abstract public void copyTo(final Self dest, long length);

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    abstract public long size();

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that reference this array.
     * You have to {@link HugeCursor#close()} every cursor instance as well.
     * <p>
     * The amount is not removed from the {@link AllocationTracker} that had been provided in the constructor.
     *
     * @return the amount of memory freed, in bytes.
     */
    abstract public long release();

    /**
     * Returns a new {@link HugeCursor} for this array. The cursor is not positioned and in an invalid state.
     * You must call {@link HugeCursor#next()} first to position the cursor to a valid state.
     *
     * Obtaining a {@link HugeCursor} for an empty array (where {@link #size()} returns {@code 0}) is undefined and
     * might result in a {@link NullPointerException} or another {@link RuntimeException}.
     */
    abstract public HugeCursor<Array> newCursor();

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
    final public HugeCursor<Array> cursor(HugeCursor<Array> cursor) {
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
     * @see HugeIntArray#cursor(HugeCursor)
     */
    final public HugeCursor<Array> cursor(HugeCursor<Array> cursor, long start, long end) {
        assert start >= 0L && start <= size() : "start expected to be in [0 : " + size() + "] but got " + start;
        assert end >= start && end <= size() : "end expected to be in [" + start + " : " + size() + "] but got " + end;
        cursor.setRange(start, end);
        return cursor;
    }

    /**
     * @return the value at the given index
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract Box boxedGet(long index);

    /**
     * Sets the value at the given index to the given value.
     *
     * @throws ArrayIndexOutOfBoundsException if the index is not within {@link #size()}
     */
    abstract void boxedSet(long index, Box value);

    /**
     * Set all elements using the provided generator function to compute each element.
     * <p>
     * The behavior is identical to {@link Arrays#setAll(int[], java.util.function.IntUnaryOperator)}.
     */
    abstract void boxedSetAll(LongFunction<Box> gen);

    /**
     * Assigns the specified value to each element.
     * <p>
     * The behavior is identical to {@link Arrays#fill(int[], int)}.
     */
    abstract void boxedFill(Box value);

    /**
     * @return the contents of this array as a flat java primitive array.
     *         The returned array might be shared and changes would then
     *         be reflected and visible in this array.
     * @throws IllegalStateException if the array is too large
     */
    abstract public Array toArray();

    @Override
    public String toString() {
        if (size() == 0L) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        try (HugeCursor<Array> cursor = cursor(newCursor())) {
            while (cursor.next()) {
                Array array = cursor.array;
                for (int i = cursor.offset; i < cursor.limit; ++i) {
                    sb.append(java.lang.reflect.Array.get(array, i)).append(", ");
                }
            }
        }
        sb.setLength(sb.length() - 1);
        sb.setCharAt(sb.length() - 1, ']');
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    Array dumpToArray(final Class<Array> componentClass) {
        long fullSize = size();
        if ((long) (int) fullSize != fullSize) {
            throw new IllegalStateException("array with " + fullSize + " elements does not fit into a Java array");
        }
        int size = (int) fullSize;
        Object result = java.lang.reflect.Array.newInstance(componentClass.getComponentType(), size);
        int pos = 0;
        try (HugeCursor<Array> cursor = cursor(newCursor())) {
            while (cursor.next()) {
                Array array = cursor.array;
                int length = cursor.limit - cursor.offset;
                System.arraycopy(array, cursor.offset, result, pos, length);
                pos += length;
            }
        }
        return (Array) result;
    }
}
