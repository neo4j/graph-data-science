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

import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * iterates over a range of int values in random order
 * using a https://en.wikipedia.org/wiki/Linear_congruential_generator
 * without having to have all the numbers in memory.
 *
 * The iterator can be reset, but not re-sized.
 * The iteration order does not change after resetting.
 */
public final class RandomIntIterator implements PrimitiveIntIterator {
    private final RandomLongIterator longs;

    /**
     * @param end iteration end, exclusive
     */
    public RandomIntIterator(int end) {
        this(0, end, ThreadLocalRandom.current());
    }

    /**
     * @param start iteration start, inclusive
     * @param end   iteration end, exclusive
     */
    public RandomIntIterator(int start, int end) {
        this(start, end, ThreadLocalRandom.current());
    }

    /**
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomIntIterator(int end, Random random) {
        this(0, end, random);
    }

    /**
     * @param start  iteration start, inclusive
     * @param end    iteration end, exclusive
     * @param random random instance to provide the initial seed
     */
    public RandomIntIterator(int start, int end, Random random) {
        longs = new RandomLongIterator(start, end, random);
    }

    RandomIntIterator(RandomLongIterator longs) {
        this.longs = longs;
    }

    @Override
    public boolean hasNext() {
        return longs.hasNext();
    }

    @Override
    public int next() {
        return (int) longs.next();
    }

    /**
     * Reset the iterator to the beginning of this iteration.
     */
    public void reset() {
        longs.reset();
    }
}
