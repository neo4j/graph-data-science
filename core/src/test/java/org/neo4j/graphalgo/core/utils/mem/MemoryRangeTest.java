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
package org.neo4j.graphalgo.core.utils.mem;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public final class MemoryRangeTest {

    @Test
    public void testFactoryMethods() {
        MemoryRange range;

        range = MemoryRange.of(42);
        assertEquals(42L, range.min);
        assertEquals(42L, range.max);

        range = MemoryRange.of(42, 1337);
        assertEquals(42L, range.min);
        assertEquals(1337L, range.max);
    }

    @Test
    public void rangeMustNotBeNegative() {
        try {
            MemoryRange.of(-42);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("min range < 0: -42", e.getMessage());
        }
        try {
            MemoryRange.of(42, -1337);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("max range < 0: -1337", e.getMessage());
        }
    }

    @Test
    public void minMustBeSmallerThanMax() {
        try {
            MemoryRange.of(1337, 42);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("max range < min: 42 < 1337", e.getMessage());
        }
    }

    @Test
    public void emptyHasZeroMinMax() {
        MemoryRange empty = MemoryRange.empty();
        assertEquals(0L, empty.min);
        assertEquals(0L, empty.max);
    }

    @Test
    public void isEmptyChecksForZeroMinMax() {
        MemoryRange empty = MemoryRange.empty();
        assertTrue(empty.isEmpty());

        empty = MemoryRange.of(0L);
        assertTrue(empty.isEmpty());

        empty = MemoryRange.of(0L, 0L);
        assertTrue(empty.isEmpty());
    }

    @Test
    public void equalsChecksForValueEquality() {
        MemoryRange range1 = MemoryRange.of(42);
        MemoryRange range2 = MemoryRange.of(42);
        MemoryRange range3 = MemoryRange.of(42, 1337);

        assertEquals(range1, range2);
        assertEquals(range2, range1); // symmetric
        assertNotEquals(range1, range3);
    }

    @Test
    public void addAddsTheirMinAndMaxValues() {
        MemoryRange range1 = MemoryRange.of(42);
        MemoryRange range2 = MemoryRange.of(1337);
        assertEquals(MemoryRange.of(42 + 1337), range1.add(range2));

        MemoryRange range3 = MemoryRange.of(42, 1337);
        assertEquals(MemoryRange.of(42 + 42, 1337 + 42), range3.add(range1));
        assertEquals(MemoryRange.of(42 + 42, 1337 + 1337), range3.add(range3));
    }

    @Test
    public void additionLaws() {
        MemoryRange range1 = MemoryRange.of(42);
        MemoryRange range2 = MemoryRange.of(1337);
        MemoryRange range3 = MemoryRange.of(42, 1337);

        // Commutativity
        assertEquals(range1.add(range2), range2.add(range1));
        // Associativity
        assertEquals(range1.add(range2).add(range3), range1.add(range2.add(range3)));
        // Identity
        assertEquals(range1, range1.add(MemoryRange.empty()));
        assertEquals(range1, MemoryRange.empty().add(range1));
    }

    @Test(expected = ArithmeticException.class)
    public void addFailsOnOverflow() {
        MemoryRange.of(Long.MAX_VALUE).add(MemoryRange.of(42));
    }

    @Test
    public void timesMultipliesTheirMinAndMaxValues() {
        MemoryRange range = MemoryRange.of(42);
        assertEquals(MemoryRange.of(42 * 1337), range.times(1337));

        range = MemoryRange.of(42, 1337);
        assertEquals(MemoryRange.of(42 * 42, 1337 * 42), range.times(42));
        assertEquals(MemoryRange.of(42 * 1337, 1337 * 1337), range.times(1337));
    }

    @Test
    public void multiplicationLaws() {
        MemoryRange range1 = MemoryRange.of(42);
        MemoryRange range2 = MemoryRange.of(1337);
        MemoryRange range3 = MemoryRange.of(42, 1337);

        // Commutativity
        assertEquals(range1.times(range2.min), range2.times(range1.min));
        // Associativity
        assertEquals(range1.times(range2.min).times(range3.min), range1.times(range2.times(range3.min).min));
        // Identity
        assertEquals(range1, range1.times(1));
        // Zero Property
        assertEquals(MemoryRange.empty(), range1.times(0));
        assertEquals(MemoryRange.empty(), MemoryRange.empty().times(42));
    }

    @Test(expected = ArithmeticException.class)
    public void timesFailsOnOverflow() {
        MemoryRange.of(Long.MAX_VALUE / 2).times(3);
    }

    @Test
    public void toStringProducesSingleHumanReadableOutputIfMinEqualsMax() {
        assertEquals("42 Bytes", MemoryRange.of(42).toString());
        assertEquals("1337 Bytes", MemoryRange.of(1337).toString());
        assertEquals("54 KiB", MemoryRange.of(1337 * 42).toString());
        assertEquals("124 GiB", MemoryRange.of(133_742_133_742L).toString());
        assertEquals("8191 PiB", MemoryRange.of(Long.MAX_VALUE).toString());
    }

    @Test
    public void toStringProducesHumanReadableRangeOutput() {
        assertEquals("[42 Bytes ... 1337 Bytes]", MemoryRange.of(42, 1337).toString());
        assertEquals("[54 KiB ... 124 GiB]", MemoryRange.of(1337 * 42, 133_742_133_742L).toString());
    }
}
