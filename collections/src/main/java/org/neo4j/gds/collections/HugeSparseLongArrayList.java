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

import java.util.stream.Stream;

/**
 * A long-indexable version of a list of long arrays that can
 * contain more than 2bn. elements and is growable.
 * <p>
 * It is implemented by paging of smaller arrays where each array, a so-called
 * page, can store up to 4096 elements. Using small pages can lead to fewer
 * array allocations if the value distribution is sparse. For indices for which
 * no value has been inserted, a user-defined default value is returned.
 * <p>
 * The list is mutable and not thread-safe.
 */
@HugeSparseList(
    valueType = long[].class,
    forAllConsumerType = LongLongArrayConsumer.class
)
public interface HugeSparseLongArrayList {

    static HugeSparseLongArrayList of(long[] defaultValue) {
        return of(defaultValue, 0);
    }

    static HugeSparseLongArrayList of(long[] defaultValue, long initialCapacity) {
        return new HugeSparseLongArrayListSon(defaultValue, initialCapacity);
    }

    /**
     * @return the current maximum number of values that can be stored in the list
     */
    long capacity();

    /**
     * @return true, iff the value at the given index is not the default value
     */
    boolean contains(long index);

    /**
     * @return the long array at the given index
     */
    long[] get(long index);

    /**
     * Sets the value at the given index.
     */
    void set(long index, long[] value);

    /**
     * Applies to given consumer to all non-default values stored in the list.
     */
    void forAll(LongLongArrayConsumer consumer);

    /**
     * Returns an iterator that consumes the underlying pages of this list.
     * Once the iterator has been consumed, the list is empty and will return
     * the default value for each index.
     */
    DrainingIterator<long[][]> drainingIterator();

    Stream<long[]> stream();
}
