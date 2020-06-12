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

import static org.junit.jupiter.api.Assertions.*;

class LongPageFillerTest {

    @Test
    void fillsAnArray() {
        long[] array = {1, 3, 3, 7};

        LongPageFiller.of(1, (d) -> 42L).accept(array);

        long[] expected = {42, 42, 42, 42};
        assertArrayEquals(expected, array);
    }

    @Test
    void doesNotFillAnything() {
        long[] array = {1, 3, 3, 7};

        LongPageFiller.passThrough().accept(array);
        assertArrayEquals(array, array);
    }

    @Test
    void fillsWithIndex() {
        long[] array = {1, 3, 3, 7};

        LongPageFiller.identity(4).accept(array);

        long[] expected = {0, 1, 2, 3};
        assertArrayEquals(expected, array);
    }
}