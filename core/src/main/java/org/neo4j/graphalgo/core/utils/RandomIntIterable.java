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

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Random;

/**
 * iterates over a range of int values in random order
 * using a https://en.wikipedia.org/wiki/Linear_congruential_generator
 * without having to have all the numbers in memory.
 *
 * If the delegate is created with
 * {@link RandomLongIterable#RandomLongIterable(long, long, Random, boolean)}
 * and {@code true} as last parameter, the iterator returned on every call
 * to {@link #iterator()} will be the same and will provide the
 * same iteration order and cannot be shared.
 */
public final class RandomIntIterable implements PrimitiveIntIterable {

    private final RandomIntIterator iterator;
    private final RandomLongIterable longs;

    public RandomIntIterable(RandomLongIterable longs) {
        this.longs = longs;
        iterator = null;
    }

    @Override
    public PrimitiveIntIterator iterator() {
        RandomIntIterator iterator = this.iterator;
        if (iterator == null) {
            iterator = new RandomIntIterator((RandomLongIterator) longs.iterator());
        } else {
            iterator.reset();
        }
        return iterator;
    }

    public RandomIntIterator iterator(Random newRandom) {
        return new RandomIntIterator(longs.iterator(newRandom));
    }
}
