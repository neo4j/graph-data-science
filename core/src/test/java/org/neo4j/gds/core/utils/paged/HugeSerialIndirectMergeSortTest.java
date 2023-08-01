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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.collections.ha.HugeLongArray;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class HugeSerialIndirectMergeSortTest {

    static Stream<Arguments> sizeAndConcurrency() {
        var random = new Random();
        return TestSupport.crossArguments(
            // array sizes
            () -> Stream.concat(
                Stream.concat(
                    random.longs(5, 1, 100).boxed(),
                    random.longs(5, 100, 1_000).boxed()
                ),
                Stream.concat(
                    random.longs(5, 1_000, 10_000).boxed(),
                    random.longs(5, 10_000, 1_000_000).boxed()
                )
            ).map(Arguments::of),
            // single vs paged array
            () -> List.of(true, false).stream().map(Arguments::of)
        );
    }

    @ParameterizedTest
    @MethodSource("sizeAndConcurrency")
    void sortArray(long size, boolean useSingleArray) {
        var array = useSingleArray
            ? HugeLongArray.newSingleArray((int) size)
            : HugeLongArray.newPagedArray(size);
        var longs = new Random().longs(size, 0, size).toArray();
        var doubles = new Random().doubles(size).toArray();
        for (int i = 0; i < size; i++) {
            array.set(i, longs[i]);
        }

        HugeSerialIndirectMergeSort.sort(array, l -> doubles[(int) l]);

        var Longs = ArrayUtils.toObject(longs);
        Arrays.sort(Longs, Comparator.comparingDouble(lhs -> doubles[(int) (long) lhs]));
        for (int i = 0; i < array.size(); i++) {
            assertThat(Longs[i]).isEqualTo(array.get(i));
        }
    }

}
