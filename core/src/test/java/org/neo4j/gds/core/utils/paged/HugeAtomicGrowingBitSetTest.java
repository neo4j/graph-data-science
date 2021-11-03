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
package org.neo4j.gds.core.utils.paged;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import static org.assertj.core.api.Assertions.assertThat;

class HugeAtomicGrowingBitSetTest {

    @Test
    void testGrowEmpty() {
        var atomicBitSet = HugeAtomicGrowingBitSet.create(0, AllocationTracker.empty());
        atomicBitSet.set(0);
        assertThat(atomicBitSet.get(0)).isTrue();
    }

    @Test
    void testGrow() {
        var atomicBitSet = HugeAtomicGrowingBitSet.create(42, AllocationTracker.empty());

        assertThat(atomicBitSet.size()).isEqualTo(42);
        atomicBitSet.set(42);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(48);

        atomicBitSet.set(1337);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.get(1337)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(1505);

        atomicBitSet.set(42_1337);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.get(1337)).isTrue();
        assertThat(atomicBitSet.get(42_1337)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(474_005);

        atomicBitSet.set(42_1337_1337L);
        assertThat(atomicBitSet.get(42)).isTrue();
        assertThat(atomicBitSet.get(1337)).isTrue();
        assertThat(atomicBitSet.get(42_1337)).isTrue();
        assertThat(atomicBitSet.get(42_1337_1337L)).isTrue();
        assertThat(atomicBitSet.size()).isEqualTo(4_740_042_755L);
    }
}
