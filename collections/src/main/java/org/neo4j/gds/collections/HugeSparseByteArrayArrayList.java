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

/**
 * A long-indexable version of a list of byte arrays that can
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
    valueType = byte[][].class,
    forAllConsumerType = LongByteArrayArrayConsumer.class
)
public interface HugeSparseByteArrayArrayList extends HugeSparseObjectArrayList<byte[][], LongByteArrayArrayConsumer> {

    static HugeSparseByteArrayArrayList of(byte[][] defaultValue) {
        return of(defaultValue, 0);
    }

    static HugeSparseByteArrayArrayList of(byte[][] defaultValue, long initialCapacity) {
        return new HugeSparseByteArrayArrayListSon(defaultValue, initialCapacity);
    }
}
