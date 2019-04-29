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
package org.neo4j.graphalgo.core.utils.paged;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class PagedSimpleBitSetTest {

    private final PagedSimpleBitSet set = PagedSimpleBitSet.newBitSet(Integer.MAX_VALUE + 100L, AllocationTracker.EMPTY);

    @Test
    public void testLowValues() throws Exception {
        assertFalse(set.contains(123));
        set.put(123);
        assertTrue(set.contains(123));
        set.clear();
        assertFalse(set.contains(123));
    }

    @Test
    public void testHighValues() throws Exception {
        assertFalse(set.contains(Integer.MAX_VALUE + 42L));
        set.put(Integer.MAX_VALUE + 42L);
        assertTrue(set.contains(Integer.MAX_VALUE + 42L));
        set.clear();
        assertHighEmpty();
    }

    private void assertHighEmpty() {
        for (long i = 0; i < 100; i++) {
            assertFalse(set.contains(Integer.MAX_VALUE + i));
        }
    }

}