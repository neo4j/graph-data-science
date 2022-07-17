/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

class AdjacencyPackingTest {

    @Test
    void name() {
        var mem = UnsafeUtil.allocateMemory(16, EmptyMemoryTracker.INSTANCE);

        UnsafeUtil.putLong(mem, 0x1122_3344_5566_7788L);
        UnsafeUtil.putLong(mem + 8, 0x42L);

        var i1 = UnsafeUtil.getInt(mem);
        System.out.printf("i1 = %X%n", i1);
        var i2 = UnsafeUtil.getInt(mem + 4);
        System.out.printf("i2 = %X%n", i2);
        var i3 = UnsafeUtil.getInt(mem + 8);
        System.out.printf("i3 = %X%n", i3);
        var i4 = UnsafeUtil.getInt(mem + 12);
        System.out.printf("i4 = %X%n", i4);

        UnsafeUtil.free(mem, 16, EmptyMemoryTracker.INSTANCE);

    }

}
