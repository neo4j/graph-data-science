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

@HugeSparseList(
    valueType = int.class,
    forAllConsumerType = LongIntConsumer.class
)
public interface HugeSparseIntList {

    static HugeSparseIntList of(int defaultValue) {
        return of(defaultValue, 0);
    }

    static HugeSparseIntList of(int defaultValue, long initialCapacity) {
        return new HugeSparseIntListSon(defaultValue, initialCapacity);
    }

    long capacity();

    boolean contains(long index);

    int get(long index);

    void set(long index, int value);

    boolean setIfAbsent(long index, int value);

    void addTo(long index, int value);

    void forAll(LongIntConsumer consumer);

    DrainingIterator<int[]> drainingIterator();

}
