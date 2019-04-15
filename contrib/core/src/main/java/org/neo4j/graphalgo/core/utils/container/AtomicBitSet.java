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
package org.neo4j.graphalgo.core.utils.container;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * thread safe BitSet based on AtomicInts.
 * impl. taken from https://stackoverflow.com/questions/12424633/atomicbitset-implementation-for-java
 */
public class AtomicBitSet {

    private final AtomicIntegerArray elements;

    public AtomicBitSet(int length) {
        elements = new AtomicIntegerArray((length + 31) >>> 5);
    }

    /**
     * reset the bitset
     */
    public void clear() {
        for (int i = elements.length() - 1; i >= 0; i--) {
            elements.set(i, 0);
        }
    }

    /**
     * set n
     *
     * @param n
     */
    public void set(long n) {
        int bit = 1 << n;
        int index = (int) (n >>> 5);
        while (true) {
            int current = elements.get(index);
            int value = current | bit;
            if (current == value || elements.compareAndSet(index, current, value))
                return;
        }
    }

    /**
     * try to set n
     *
     * @param n
     * @return true if successfully set n, false otherwise (another thread did it)
     */
    public boolean trySet(long n) {
        int bit = 1 << n;
        int index = (int) (n >>> 5);
        int current, value;
        do {
            current = elements.get(index);
            value = current | bit;
            if (current == value) {
                return false;
            }
        } while (!elements.compareAndSet(index, current, value));
        return true;
    }

    /**
     * unset n
     *
     * @param n
     */
    public void unset(long n) {
        final int bit = ~(1 << n);
        final int index = (int) (n >>> 5);
        while (true) {
            final int current = elements.get(index);
            final int value = current & bit;
            if (current == value || elements.compareAndSet(index, current, value))
                return;
        }
    }

    /**
     * get state of bit n
     *
     * @param n the biut
     * @return its state
     */
    public boolean get(long n) {
        final int bit = 1 << n;
        final int index = (int) (n >>> 5);
        final int value = elements.get(index);
        return (value & bit) != 0;
    }
}
