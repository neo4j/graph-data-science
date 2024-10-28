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
package org.neo4j.gds.api;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.primitive.PrimitiveLongIterable;

import java.util.Collection;
import java.util.PrimitiveIterator;

/**
 * Iterate over each graph-nodeId in batches.
 */
public interface BatchNodeIterable {

    /**
     * @return a collection of iterables over every node, partitioned by
     *         the given batch size.
     */
    Collection<PrimitiveLongIterable> batchIterables(long batchSize);

    final class IdIterable implements PrimitiveLongIterable {
        private final long start;
        private final long length;

        public IdIterable(long start, long length) {
            this.start = start;
            this.length = length;
        }

        @Override
        public PrimitiveIterator.OfLong iterator() {
            return new IdIterator(start, length);
        }
    }

    final class IdIterator implements PrimitiveIterator.OfLong {

        private long current;
        private final long limit; // exclusive upper bound

        public IdIterator(long length) {
            this.current = 0;
            this.limit = length;
        }

        private IdIterator(long start, long length) {
            this.current = start;
            this.limit = start + length;
        }

        @Override
        public boolean hasNext() {
            return current < limit;
        }

        @Override
        public long nextLong() {
            return current++;
        }
    }

    final class BitSetIdIterator implements PrimitiveIterator.OfLong {
        private final BitSet labelBitSet;

        long position;

        public BitSetIdIterator(BitSet labelBitSet) {
            this.labelBitSet = labelBitSet;
            this.position = labelBitSet.nextSetBit(0);
        }

        @Override
        public boolean hasNext() {
            return position >= 0;
        }

        @Override
        public long nextLong() {
            var tmp = this.position;
            this.position = labelBitSet.nextSetBit(this.position + 1);
            return tmp;
        }
    }
}
