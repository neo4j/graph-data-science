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
package org.neo4j.graphalgo.core.utils.paged;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class HugeMergeSortTest {

    @ParameterizedTest
    @ValueSource(longs = {10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000})
    void sortArray(long size) {
        var tracker = AllocationTracker.empty();

        var array = HugeLongArray.newArray(size, tracker);

        var random = new Random();

        for (long i = 0; i < size; i++) {
            array.set(i, random.nextLong());
        }

        HugeMergeSort.sort(array, 16, tracker);

        for (int i = 1; i < array.size(); i++) {
            assertThat(array.get(i)).isGreaterThan(array.get(i - 1));
        }

    }

}
