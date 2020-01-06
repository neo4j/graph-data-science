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
import org.neo4j.graphalgo.core.utils.SetBitsIterable;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SetBitsIterableTest {

    @Test
    void shouldIterateSparseBitSet() {
        long bound = 10;
        List<Long> expected = Arrays.asList(0L, 2L, 4L, 6L, 8L);
        BitSet bitSet = new BitSet(bound);
        LongStream.range(0, bound).filter(l -> l % 2 == 0).forEach(bitSet::set);

        List<Long> actual = new SetBitsIterable(bitSet).stream().boxed().collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void shouldIterateDenseBitset() {
        long bound = 10;

        List<Long> expected = Arrays.asList(0L, 1L, 2L, 3L, 4L);
        BitSet bitSet = new BitSet(10);

        LongStream.range(0, bound).limit(5).forEach(bitSet::set);

        List<Long> actual = new SetBitsIterable(bitSet).stream().boxed().collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void shouldIterateCompleteBitset() {
        long bound = 100;
        List<Long> expected = LongStream.range(0, bound).boxed().collect(Collectors.toList());

        BitSet bitSet = new BitSet(bound);

        LongStream.range(0, bound).forEach(bitSet::set);

        List<Long> actual = new SetBitsIterable(bitSet).stream().boxed().collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void shouldIHandleMultipleIteratorsOverSameBitSet() {
        long bound = 10;
        List<String> expected = Arrays.asList("0, 2", "0, 4", "0, 6", "0, 8", "2, 4", "2, 6", "2, 8", "4, 6", "4, 8", "6, 8");

        BitSet bitSet = new BitSet(bound);
        LongStream.range(0, bound).filter(l -> l % 2 == 0).forEach(bitSet::set);

        LongStream outerIter = new SetBitsIterable(bitSet).stream();
        List<String> actual = outerIter.boxed().flatMap(
            outerVal -> new SetBitsIterable(bitSet, outerVal + 1).stream()
                .mapToObj(innerVal -> String.format("%d, %d", outerVal, innerVal))
        ).collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void shouldIterateWithOffset() {
        long bound = 10;
        List<Long> expected = Arrays.asList(5L, 6L, 7L, 8L, 9L);

        BitSet bitSet = new BitSet(bound);

        LongStream.range(0, bound).forEach(bitSet::set);

        List<Long> actual = new SetBitsIterable(bitSet, 5).stream().boxed().collect(Collectors.toList());
        assertEquals(expected, actual);
    }

    @Test
    void shouldIterateWithOffsetPastLastSetBit() {
        long bound = 10;

        BitSet bitSet = new BitSet(bound);

        LongStream.range(0, bound).forEach(bitSet::set);

        List<Long> actual = new SetBitsIterable(bitSet, bound).stream().boxed().collect(Collectors.toList());
        assertEquals(emptyList(), actual);
    }

    @Test
    void shouldOnlyParallelizeOnTheOuterStream() {
        int endExclusive = 1_000;

        BitSet bitSet = new BitSet(endExclusive);
        LongStream.range(0, endExclusive).forEach(bitSet::set);

        // When parallelizing the outer stream and collecting one result per outerVal and thread
        Set<String> actual = new SetBitsIterable(bitSet).stream().parallel().boxed().flatMap(
            outerVal -> new SetBitsIterable(bitSet, outerVal + 1).stream()
                .mapToObj(innerVal -> String.format("%d handled by %s", outerVal, Thread.currentThread().getName()))
        ).collect(Collectors.toSet());

        // There should be exactly one result per outerVal, because each inner iteration is handled by exactly one thread
        int expected = endExclusive - 1;

        assertEquals(expected, actual.size());
    }
}
