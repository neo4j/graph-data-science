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

import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class HugeMergeSortTest {

    static ArgumentSets sizeAndConcurrency() {
        var random = new Random();
        return ArgumentSets
            .argumentsForFirstParameter(
                Stream.concat(
                    Stream.concat(
                        random.longs(5, 1, 100).boxed(),
                        random.longs(5, 100, 1_000).boxed()
                    ),
                    Stream.concat(
                        random.longs(5, 1_000, 10_000).boxed(),
                        random.longs(5, 10_000, 1_000_000).boxed()
                    )
                )
            )
            .argumentsForNextParameter(Stream.of(1, 4).map(Concurrency::new))
            .argumentsForNextParameter(true, false);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("sizeAndConcurrency")
    void sortArray(long size, Concurrency concurrency, boolean useSingleArray) {
        var array = useSingleArray
            ? HugeLongArray.newSingleArray((int) size)
            : HugeLongArray.newPagedArray(size);
        var longs = new Random().longs(size).toArray();
        for (int i = 0; i < size; i++) {
            array.set(i, longs[i]);
        }

        HugeMergeSort.sort(array, concurrency);

        Arrays.sort(longs);
        for (int i = 0; i < array.size(); i++) {
            assertThat(longs[i]).isEqualTo(array.get(i));
        }
    }

}
