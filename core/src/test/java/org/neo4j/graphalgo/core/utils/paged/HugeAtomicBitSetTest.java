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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeAtomicBitSetTest {

    @Test
    void testGetSetClear() {
        var bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
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
    void setReturnsTrueIfTheBitWasUnset() {
        var bitSet = HugeAtomicBitSet.create(1, AllocationTracker.empty());
        assertTrue(bitSet.set(0));
    }

    @Test
    void setReturnsFalseIfTheBitWasSet() {
        var bitSet = HugeAtomicBitSet.create(1, AllocationTracker.empty());
        bitSet.set(0);
        assertFalse(bitSet.set(0));
    }


    @Test
    void testFlipping() {
        var bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
        bitSet.flip(41);
        assertTrue(bitSet.get(41));
        bitSet.flip(41);
        assertFalse(bitSet.get(41));
    }

    @Test
    void testCardinality() {
        var bitSet = HugeAtomicBitSet.create(42, AllocationTracker.EMPTY);
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
        var bitSet = HugeAtomicBitSet.create(100, AllocationTracker.EMPTY);
        for (long i = 0; i < bitSet.size(); i++) {
            bitSet.set(i);
        }

        bitSet.clear();

        for (long i = 0; i < bitSet.size(); i++) {
            assertFalse(bitSet.get(i));
        }
    }
}
