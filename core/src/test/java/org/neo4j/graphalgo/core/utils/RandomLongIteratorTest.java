/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RandomLongIteratorTest {

    @FunctionalInterface
    interface BiLongFunction<R> {
        R apply(long start, long end);
    }

    @Test
    void shouldRandomlyEmitNumbers() {
        testIterator(0L, 10L);
    }

    @Test
    void shouldSupportNonZeroStarts() {
        testIterator(13L, 37L);
    }

    @Test
    void shouldFailOnNegativeRange() {
        assertThrows(IndexOutOfBoundsException.class, () -> testIterator(37L, 13L));
    }

    @Test
    void shouldEmitLargeSequences() {
        testIterator(1337L, 420_000_000L);
    }

    private void testIterator(long start, long end) {
        testIterator(start, end,
                RandomLongIterator::new,
                RandomLongIterator::hasNext,
                RandomLongIterator::next);
    }

    static <T> void testIterator(
            long start,
            long end,
            BiLongFunction<T> construct,
            Predicate<T> hasNext,
            ToLongFunction<T> next) {
        T iter = construct.apply(start, end);
        long expectedCount = end - start;
        BigInteger expectedBigSum = BigInteger
                .valueOf(expectedCount)
                .multiply(BigInteger.valueOf(start).add(BigInteger.valueOf(end)).subtract(BigInteger.ONE))
                .divide(BigInteger.valueOf(2L));
        long expectedSum = expectedBigSum.longValueExact();
        BitSet seen = new BitSet(end);
        List<Long> underflow = new ArrayList<>();
        List<Long> overflow = new ArrayList<>();
        long collisions = 0L;
        long count = 0L;
        long sum = 0L;
        while (hasNext.test(iter)) {
            long value = next.applyAsLong(iter);
            sum += value;
            ++count;
            if (seen.getAndSet(value)) {
                ++collisions;
            }
            if (value < start) {
                underflow.add(value);
            }
            if (value >= end) {
                overflow.add(value);
            }
        }
        assertEquals(0L, collisions, "Should not produce collisions, but got " + collisions);
        assertEquals(expectedCount, count, "Should emit exactly " + expectedCount + " values, but got " + count);
        assertTrue(underflow.isEmpty(), "All values should be greater than " + (start - 1L) + " but got " + underflow);
        assertTrue(overflow.isEmpty(), "All values should be lower than " + end + " but got " + overflow);
        assertEquals(expectedSum, sum, "Should add up to " + expectedSum + " but got " + sum);
    }
}
