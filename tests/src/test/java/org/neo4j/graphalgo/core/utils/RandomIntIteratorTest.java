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

import org.junit.Test;

public final class RandomIntIteratorTest {

    @Test
    public void shouldRandomlyEmitNumbers() {
        testIterator(0, 10);
    }

    @Test
    public void shouldSupportNonZeroStarts() {
        testIterator(13, 37);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldFailOnNegativeRange() {
        testIterator(37, 13);
    }

    @Test
    public void shouldEmitLargeSequences() {
        testIterator(1337, 420_000_000);
    }

    private void testIterator(int start, int end) {
        RandomLongIteratorTest.testIterator(
                (long) start, (long) end,
                (v1, v2) -> new RandomIntIterator((int) v1, (int) v2),
                RandomIntIterator::hasNext,
                RandomIntIterator::next);
    }
}
