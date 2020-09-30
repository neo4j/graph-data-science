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
package org.neo4j.graphalgo.core.utils.paged;

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeAtomicBitSetTest {

    @Test
    void testGetSetClear() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
        assertFalse(bitSet.get(7));
        assertFalse(bitSet.get(8));
        assertFalse(bitSet.get(9));
        bitSet.set(8);
        assertFalse(bitSet.get(7));
        assertTrue(bitSet.get(8));
        assertFalse(bitSet.get(9));
        bitSet.clear(8);
        assertFalse(bitSet.get(7));
        assertFalse(bitSet.get(8));
        assertFalse(bitSet.get(9));
    }

    @Test
    void getAndSetReturnsTrueIfTheBitWasSet() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(1, AllocationTracker.EMPTY);
        bitSet.set(0);
        assertTrue(bitSet.getAndSet(0));
    }

    @Test
    void getAndSetReturnsFalseIfTheBitWasNotSet() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(1, AllocationTracker.EMPTY);
        assertFalse(bitSet.getAndSet(0));
    }

    @Test
    void getAndSetSetsTheBit() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(1, AllocationTracker.EMPTY);
        assertFalse(bitSet.get(0));
        bitSet.getAndSet(0);
        assertTrue(bitSet.get(0));
    }

    @ParameterizedTest
    @CsvSource({"0,41", "0,7", "10,20"})
    void setRange(int startIndex, int endIndex) {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
        bitSet.set(startIndex, endIndex);
        for (int i = 0; i < bitSet.capacity(); i++) {
            if (i < startIndex || i > endIndex) {
                assertFalse(bitSet.get(i), String.format(Locale.US, "index %d expected to be false", i));
            } else {
                assertTrue(bitSet.get(i), String.format(Locale.US, "index %d expected to be true", i));
            }
        }
    }

    @Test
    void testFlipping() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
        bitSet.flip(41);
        assertTrue(bitSet.get(41));
        bitSet.flip(41);
        assertFalse(bitSet.get(41));
    }

    @Test
    void testCardinality() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
        assertEquals(0L, bitSet.cardinality());

        bitSet.set(41);
        assertEquals(1L, bitSet.cardinality());

        for (long i = 0; i < bitSet.size(); i++) {
            bitSet.set(i);
        }
        assertEquals(42L, bitSet.cardinality());
    }

    @Test
    void testClearAll() {
        HugeAtomicBitSet bitSet = HugeAtomicBitSet.create(100, AllocationTracker.EMPTY);
        for (long i = 0; i < bitSet.size(); i++) {
            bitSet.set(i);
        }

        bitSet.clear();

        for (long i = 0; i < bitSet.size(); i++) {
            assertFalse(bitSet.get(i));
        }
    }

    @Test
    void testToBitSet() {
        HugeAtomicBitSet atomicBitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
        atomicBitSet.set(1);
        atomicBitSet.set(9);
        atomicBitSet.set(8);
        atomicBitSet.set(4);

        BitSet bitSet = atomicBitSet.toBitSet();
        assertEquals(atomicBitSet.cardinality(), bitSet.cardinality());
        assertTrue(bitSet.get(1));
        assertTrue(bitSet.get(9));
        assertTrue(bitSet.get(8));
        assertTrue(bitSet.get(4));

        bitSet.set(43);
    }
}
