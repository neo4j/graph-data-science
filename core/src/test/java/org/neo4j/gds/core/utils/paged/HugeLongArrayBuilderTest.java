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
import org.neo4j.gds.mem.HugeArrays;

import java.util.Arrays;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class HugeLongArrayBuilderTest {

    @Test
    void shouldInsertIdsInSinglePage() {
        var idMapBuilder =  HugeLongArrayBuilder.newBuilder();
        var allocator = new HugeLongArrayBuilder.Allocator();
        idMapBuilder.allocate(0, 2, allocator);
        allocator.insert(new long[]{ 42, 1337 });

        var array = idMapBuilder.build(2);

        assertThat(array.size()).isEqualTo(2);
        assertThat(array.toArray()).containsExactly(42, 1337);
    }

    @Test
    void multipleInsertsShouldAddConsecutively() {
        var idMapBuilder = HugeLongArrayBuilder.newBuilder();
        var allocator = new HugeLongArrayBuilder.Allocator();

        idMapBuilder.allocate(0, 2, allocator);
        allocator.insert(new long[]{ 42, 1337 });

        idMapBuilder.allocate(2, 2, allocator);
        allocator.insert(new long[]{ 84, 1338 });

        idMapBuilder.allocate(4, 2, allocator);
        allocator.insert(new long[]{ 126, 1339 });

        var array = idMapBuilder.build(6);

        assertThat(array.size()).isEqualTo(6);
        assertThat(array.toArray()).containsExactly(42, 1337, 84, 1338, 126, 1339);
    }

    @Test
    void multipleInsertsAcrossMultiplePages() {
        var idMapBuilder = HugeLongArrayBuilder.newBuilder();
        var allocator = new HugeLongArrayBuilder.Allocator();

        idMapBuilder.allocate(0, 2, allocator);
        allocator.insert(new long[]{ 42, 1337 });

        var batchLength = HugeArrays.PAGE_SIZE * 2 + 42;
        idMapBuilder.allocate(2, batchLength, allocator);
        var values = new long[batchLength];
        Arrays.setAll(values, i -> i % 2 == 0 ? 42L * (i / 2) : 1337L + (i / 2));
        allocator.insert(values);

        idMapBuilder.allocate(batchLength + 2, 2, allocator);
        allocator.insert(new long[]{ 42, 1337 });

        var array = idMapBuilder.build(batchLength + 4);

        assertThat(array.size()).isEqualTo(batchLength + 4);

        var expected = LongStream.concat(
            LongStream.of(42, 1337),
            LongStream.concat(Arrays.stream(values), LongStream.of(42, 1337))
        ).toArray();
        assertThat(array.toArray()).containsExactly(expected);
    }

    @Test
    void shouldInsertAllocationSizeFromPartialBatch() {
        var idMapBuilder = HugeLongArrayBuilder.newBuilder();
        var values = LongStream.range(0, 42).map(__ -> 42).toArray();

        var allocator = new HugeLongArrayBuilder.Allocator();
        idMapBuilder.allocate(0, 13, allocator);
        allocator.insert(values);

        var array = idMapBuilder.build(13);
        assertThat(array.toArray()).containsExactly(42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42);
    }

    @Test
    void multipleInsertsAreThreadSafe() throws InterruptedException {
        var idMapBuilder = HugeLongArrayBuilder.newBuilder();

        var phaser = new Phaser(3);
        var startIndex = new AtomicLong();
        var values = LongStream.range(42, 42 + 42).toArray();

        Runnable workload = () -> {
            var allocator = new HugeLongArrayBuilder.Allocator();
            phaser.arriveAndAwaitAdvance();

            for (int i = 0; i < 10_000; i++) {
                var index = startIndex.getAndAdd(42);
                idMapBuilder.allocate(index, 42, allocator);
                allocator.insert(values);
            }
        };

        var thread1 = new Thread(workload);
        var thread2 = new Thread(workload);

        thread1.start();
        thread2.start();

        // kick of threads
        phaser.arriveAndAwaitAdvance();

        thread1.join();
        thread2.join();

        var array = idMapBuilder.build(840_000);

        assertThat(array.size()).isEqualTo(840_000);

        var expected = LongStream.range(0, 20_000).flatMap(__ -> Arrays.stream(values)).toArray();

        assertArrayEquals(expected, array.toArray());
    }

}
