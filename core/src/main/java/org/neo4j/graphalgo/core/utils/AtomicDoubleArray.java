/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

/**
 * Atomic double Array implementation
 *
 * @author mknblch
 */
public class AtomicDoubleArray {

    private final AtomicLongArray data;
    private final int capacity;

    /**
     * Create a new AtomicDoubleArray
     *
     * @param capacity its capacity
     */
    public AtomicDoubleArray(int capacity) {
        this.capacity = capacity;
        data = new AtomicLongArray(capacity);
    }

    /**
     * get the value at index
     *
     * @param index the index
     * @return value at index
     */
    public double get(int index) {
        return Double.longBitsToDouble(data.get(index));
    }

    /**
     * Sets the element at position i to the given value.
     *
     * @param index the index
     * @param value the value
     */
    public void set(int index, double value) {
        data.set(index, Double.doubleToLongBits(value));
    }

    /**
     * add argument to value at index. Behaves exactly like standard java double arithmetic.
     *
     * @param index index
     * @param value value to add
     */
    public void add(int index, double value) {
        long newBits, currentBits;
        do {
            currentBits = data.get(index);
            newBits = Double.doubleToLongBits(Double.longBitsToDouble(currentBits) + value);
        } while (!data.compareAndSet(index, currentBits, newBits));
    }

    /**
     * return capacity
     *
     * @return the capacity
     */
    public int length() {
        return data.length();
    }

    /**
     * set all elements to 0
     */
    public void clear() { // TODO parallel ?!
        for (int i = data.length() - 1; i >= 0; i--) {
            data.set(i, 0);
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < data.length(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(get(i));
            if (i >= 20) {
                builder.append(", ..");
                break;
            }
        }
        return "[" + builder.toString() + "]";
    }

    public double[] toArray() {
        return IntStream.range(0, capacity)
                .mapToDouble(this::get)
                .toArray();
    }
}
