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
package org.neo4j.graphalgo.core.utils;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * iterates over a range of long values in random order
 * using a https://en.wikipedia.org/wiki/Linear_congruential_generator
 * without having to have all the numbers in memory.
 *
 * The iterator can be reset, but not re-sized.
 * The iteration order does not change after resetting.
 */
public final class RandomLongIterator implements PrimitiveLongIterator {
    private static final long multiplier = 0x5DEECE66DL;
    private static final long addend = 0xBL;

    private final long base;
    private final long range;
    private final long mask;
    private final long seed;

    private boolean hasNext;
    private long next;

    /**
     * @param end iteration end, exclusive
     */
    public RandomLongIterator(long end) {
        this(0L, end, ThreadLocalRandom.current());
    }

    /**
     * @param start iteration start, inclusive
     * @param end   iteration end, exclusive
     */
    public RandomLongIterator(long start, long end) {
        this(start, end, ThreadLocalRandom.current());
    }

    /**
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomLongIterator(long end, Random random) {
        this(0L, end, random);
    }

    /**
     * @param start  iteration start, inclusive
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomLongIterator(long start, long end, Random random) {
        long range = end - start;
        if (range < 0L || range > (1L << 62)) {
            throw new IndexOutOfBoundsException("[start, end) must not be negative or larger than " + (1L << 62));
        }
        long modulus = BitUtil.nextHighestPowerOfTwo(range);

        this.range = range;
        base = start;
        mask = modulus - 1L;
        seed = (long) random.nextInt((int) Math.min(range, (long) Integer.MAX_VALUE));
        next = seed;
        hasNext = true;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public long next() {
        next = internalNext(next, mask, range/*, seen*/);
        if (next == seed) {
            hasNext = false;
        }
        return next + base;
    }

    /**
     * Reset the iterator to the beginning of this iteration.
     */
    public void reset() {
        next = seed;
        hasNext = true;
    }

    long maxValue() {
        return base + range;
    }

    private long internalNext(long next, final long mask, final long range) {
        next = (next * multiplier + addend) & mask;
        while (next >= range) {
            next = (next * multiplier + addend) & mask;
        }
        return next;
    }
}
