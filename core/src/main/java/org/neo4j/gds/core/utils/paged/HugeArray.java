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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.collections.cursor.HugeCursorSupport;

import java.util.function.LongFunction;

public abstract class HugeArray<Array, Box, Self extends HugeArray<Array, Box, Self>> implements HugeCursorSupport<Array> {

    /**
     * Copies the content of this array into the target array.
     * <p>
     * The behavior is identical to {@link System#arraycopy(Object, int, Object, int, int)}.
     */
    public abstract void copyTo(final Self dest, long length);

    /**
     * Creates a copy of the given array. The behavior is identical to {@link java.util.Arrays#copyOf(int[], int)}.
     */
    public abstract Self copyOf(long newLength);

    /**
     * Returns the length of this array.
     * <p>
     * If the size is greater than zero, the highest supported index is {@code size() - 1}
     * <p>
     * The behavior is identical to calling {@code array.length} on primitive arrays.
     */
    public abstract long size();

    /**
     * @return the amount of memory used by the instance of this array, in bytes.
     * This should be the same as returned from {@link #release()} without actually releasing the array.
     */
    public abstract long sizeOf();

    /**
     * Destroys the data, allowing the underlying storage arrays to be collected as garbage.
     * The array is unusable after calling this method and will throw {@link NullPointerException}s on virtually every method invocation.
     * <p>
     * Note that the data might not immediately collectible if there are still cursors alive that reference this array.
     * You have to {@link org.neo4j.gds.collections.cursor.HugeCursor#close()} every cursor instance as well.
     *
     * @return the amount of memory freed, in bytes.
     */
    public abstract long release();

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
     * The behavior is identical to {@link java.util.Arrays#setAll(int[], java.util.function.IntUnaryOperator)}.
     */
    abstract void boxedSetAll(LongFunction<Box> gen);

    /**
     * Assigns the specified value to each element.
     * <p>
     * The behavior is identical to {@link java.util.Arrays#fill(int[], int)}.
     */
    abstract void boxedFill(Box value);

    /**
     * @return the contents of this array as a flat java primitive array.
     *         The returned array might be shared and changes would then
     *         be reflected and visible in this array.
     * @throws IllegalStateException if the array is too large
     */
    public abstract Array toArray();

    public abstract NodePropertyValues asNodeProperties();

    /**
     * Copies data from {@code source} into this array, starting from {@code sliceStart} up until {@code sliceEnd}.
     * @return the number of entries copied
     */
    public final int copyFromArrayIntoSlice(Array source, long sliceStart, long sliceEnd) {
        int sourceIndex = 0;
        try (HugeCursor<Array> cursor = initCursor(newCursor(), sliceStart, sliceEnd)) {
            int sourceLength = java.lang.reflect.Array.getLength(source);
            while (cursor.next() && sourceIndex < sourceLength) {
                int copyLength = Math.min(
                        cursor.limit - cursor.offset, // number of slots available in the cursor buffer
                        sourceLength - sourceIndex // number of slots left to copy from
                );
                System.arraycopy(source, sourceIndex, cursor.array, cursor.offset, copyLength);
                sourceIndex += copyLength;
            }
        }
        return sourceIndex;
    }

    @Override
    public String toString() {
        if (size() == 0L) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        try (HugeCursor<Array> cursor = initCursor(newCursor())) {
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
        try (HugeCursor<Array> cursor = initCursor(newCursor())) {
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
