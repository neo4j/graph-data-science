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
import org.neo4j.gds.core.loading.GrowingHugeIdMapBuilder;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import static org.assertj.core.api.Assertions.assertThat;

class HugeLongArrayBuilderTest {

    @Test
    void shouldInsertIdsInSinglePage() {
        var idMapBuilder = GrowingHugeIdMapBuilder.of(AllocationTracker.empty());
        var allocator = idMapBuilder.allocate(2);
        allocator.insert(new long[]{ 42, 1337 }, 2);
        var array = idMapBuilder.array();

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.toArray()).containsExactly(42, 1337);
    }

    @Test
    void multipleInsertsShouldAddConsecutively() {
        var idMapBuilder = GrowingHugeIdMapBuilder.of(AllocationTracker.empty());
        idMapBuilder.allocate(2).insert(new long[]{ 42, 1337 }, 2);

        idMapBuilder.allocate(2).insert(new long[]{ 84, 1338 }, 2);

        idMapBuilder.allocate(2).insert(new long[]{ 126, 1339 }, 2);

        var array = idMapBuilder.array();

        assertThat(array.size()).isEqualTo(6);
        assertThat(array.toArray()).containsExactly(42, 1337, 84, 1338, 126, 1339);
    }

}
