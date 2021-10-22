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
package org.neo4j.gds.collections;

import java.util.function.LongConsumer;

/**
 * A long-indexable version of a primitive float array ({@code float[]}) that
 * can contain more than 2bn. elements.
 * <p>
 * It is implemented by paging of smaller arrays where each array, a so-called
 * page, can store up to 4096 elements. Using small pages can lead to fewer
 * array allocations if the value distribution is sparse. For indices for which
 * no value has been inserted, a user-defined default value is returned.
 * <p>
 * The array is immutable and needs to be constructed using a thread-safe,
 * growing builder.
 */
@HugeSparseArray(valueType = float.class)
public interface HugeSparseFloatArray {

    /**
     * @return the maximum number of values stored in the array
     */
    long capacity();

    /**
     * @return the float value at the given index
     */
    float get(long index);

    /**
     * @return true, iff the value at the given index is not the default value
     */
    boolean contains(long index);

    /**
     * Returns an iterator that consumes the underlying pages of this array.
     * Once the iterator has been consumed, the array is empty and will return
     * the default value for each index.
     */
    DrainingIterator<float[]> drainingIterator();

    /**
     * @return a thread-safe array builder that grows dynamically on inserts
     */
    static Builder builder(float defaultValue, LongConsumer trackAllocation) {
        return new HugeSparseFloatArraySon.GrowingBuilder(defaultValue, 0, trackAllocation);
    }

    /**
     * @return a thread-safe array builder that grows dynamically on inserts
     */
    static Builder builder(float defaultValue, long initialCapacity, LongConsumer trackAllocation) {
        return new HugeSparseFloatArraySon.GrowingBuilder(defaultValue, initialCapacity, trackAllocation);
    }

    interface Builder {
        /**
         * Sets the value at the given index.
         */
        void set(long index, float value);

        /**
         * Sets the value at the given index iff it has not been set before.
         */
        boolean setIfAbsent(long index, float value);

        /**
         * Adds the given value to the value stored at the index. If no value
         * has been stored before, the value is added to the default value.
         */
        void addTo(long index, float value);

        /**
         * @return an immutable array
         */
        HugeSparseFloatArray build();
    }
}
