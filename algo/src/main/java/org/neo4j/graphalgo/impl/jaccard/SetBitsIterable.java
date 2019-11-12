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

package org.neo4j.graphalgo.impl.jaccard;

import com.carrotsearch.hppc.BitSet;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

/**
 * An iterable over the set bits in a {@link BitSet}.
 */
class SetBitsIterable implements Iterable<Long> {

    private final BitSet set;
    private final long offset;

    SetBitsIterable(BitSet set, long offset) {
        this.set = set;
        this.offset = offset;
    }

    SetBitsIterable(BitSet set) {
        this(set, 0);
    }

    @Override
    public PrimitiveIterator.OfLong iterator() {
        return new Iterator(offset);
    }

    LongStream stream() {
        return StreamSupport.longStream(spliterator(), false);
    }

    @Override
    public Spliterator.OfLong spliterator() {
        return Spliterators.spliterator(
            iterator(),
            set.cardinality(),
            Spliterator.ORDERED | Spliterator.SORTED | Spliterator.IMMUTABLE | Spliterator.NONNULL | Spliterator.DISTINCT
        );
    }

    private final class Iterator implements PrimitiveIterator.OfLong {

        long value;

        public Iterator(long index) {
            this.value = set.nextSetBit(index);
        }

        @Override
        public boolean hasNext() {
            return value > -1;
        }

        @Override
        public long nextLong() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            long returnValue = value;
            value = set.nextSetBit(value + 1);
            return returnValue;
        }

    }
}
