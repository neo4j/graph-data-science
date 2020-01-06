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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.LongRange;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HugeLongMatrixTest {

    @Property
    void testSetAndGet(
            @ForAll @LongRange(min = 0, max = 999) long x,
            @ForAll @LongRange(min = 0, max = 999) long y,
            @ForAll @LongRange(min = 0, max = 10000) long v) {
        HugeLongMatrix array = new HugeLongMatrix(1000, 1000, AllocationTracker.EMPTY);
        array.set(x, y, v);
        assertEquals(v, array.get(x, y));
    }
}
