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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.core.loading.ZigZagLongDecoding.Identity.INSTANCE;

class CompressedLongArrayStructTest {

    @Test
    void shouldWriteSingleTargetList() {
       var compressedLongArrays = new CompressedLongArrayStruct();

        var input = new long[]{ 42L, 1337L, 5L};
        compressedLongArrays.add(0, input, 0, 3, 3);

        assertThat(compressedLongArrays.length(0)).isEqualTo(3);

        var expectedTargets = new long[]{42L, 1337L, 5L};
        var actualTargets = new long[3];
        compressedLongArrays.uncompress(0, actualTargets, INSTANCE);
        assertThat(actualTargets).containsExactly(expectedTargets);
    }

    @Test
    void shouldWriteMultipleTimesIntoTargetList() {
        var compressedLongArrays = new CompressedLongArrayStruct();

        compressedLongArrays.add(0, new long[]{ 42L, 1337L, 5L}, 0, 3, 3);
        compressedLongArrays.add(0, new long[]{ 42L, 1337L, 5L}, 1, 3, 2);

        assertThat(compressedLongArrays.length(0)).isEqualTo(5);

        var expectedTargets = new long[]{42L, 1337L, 5L, 1337L, 5L};
        var actualTargets = new long[5];
        compressedLongArrays.uncompress(0, actualTargets, INSTANCE);
        assertThat(actualTargets).containsExactly(expectedTargets);
    }
}
