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
package org.neo4j.graphalgo.core.utils.container;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author mknblch
 */
class AtomicBitSetTest {

    private final AtomicBitSet set = new AtomicBitSet(Integer.MAX_VALUE);

    @Test
    void testSmallNumber() {
        assertFalse(set.get(0));
        set.set(0);
        assertTrue(set.get(0));
        set.unset(0);
        assertFalse(set.get(0));
    }

    @Test
    void testBigNumber() {
        assertFalse(set.get(Integer.MAX_VALUE));
        set.set(Integer.MAX_VALUE);
        assertTrue(set.get(Integer.MAX_VALUE));
        set.unset(Integer.MAX_VALUE);
        assertFalse(set.get(Integer.MAX_VALUE));
    }

    @Test
    void testGetInvalidValue() {
        assertThrows(IndexOutOfBoundsException.class, () -> set.get(-1));
    }

    @Test
    void testSetInvalidValue() {
        assertThrows(IndexOutOfBoundsException.class, () -> set.set(-1));
    }

    @Test
    void test() {
        final AtomicBitSet set = new AtomicBitSet(2);

        set.set(0);
        set.set(1);
        assertTrue(set.get(0));
        assertTrue(set.get(1));

        set.unset(1);
        assertTrue(set.get(0));
        assertFalse(set.get(1));

        set.set(1);
        set.unset(0);
        assertFalse(set.get(0));
        assertTrue(set.get(1));

        set.unset(0);
        set.unset(1);
        assertFalse(set.get(0));
        assertFalse(set.get(1));
    }
}
