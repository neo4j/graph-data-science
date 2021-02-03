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
package org.neo4j.graphalgo.beta.pregel;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.ArrayList;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PrimitiveSyncDoubleQueuesTest {

    @Test
    void growQueueArray() {
        var queues = PrimitiveSyncDoubleQueues.of(
            1337,
            42,
            AllocationTracker.empty()
        );

        assertThat(queues.queue(42).length).isEqualTo(42);

        for (int i = 0; i < 41; i++) {
            queues.push(42, 23);
        }

        assertThat(queues.queue(42).length).isEqualTo(42);
        queues.push(42, 1337);
        assertThat(queues.queue(42).length).isEqualTo(42);
        queues.push(42, 1337);

        assertThat(queues.queue(42).length).isEqualTo(63 /* 42 * 1.5 */);
        assertThat(queues.queue(42)[41]).isEqualTo(1337);
        assertThat(queues.queue(42)[42]).isEqualTo(1337);

        queues.push(42, 1337);
        assertThat(queues.queue(42)[43]).isEqualTo(1337);
    }

    @Test
    void parallelPush() {
        var queues = PrimitiveSyncDoubleQueues.of(1, AllocationTracker.empty());

        MutableBoolean start = new MutableBoolean(false);
        var tasks = IntStream.range(0, 4).mapToObj((taskOffset) -> (Runnable) () -> {
            while (start.isFalse()) { }
            for (int i = 0; i < 100; i++) {
                queues.push(0, i + taskOffset * 100);
            }
        }).collect(toList());

        var futures = ParallelUtil.run(tasks, false, Pools.DEFAULT, null);

        start.setTrue();

        ParallelUtil.awaitTermination(futures);

        var values = new ArrayList<Long>();
        for (long i = 0; i < queues.tail(0); i++) {
            values.add(Math.round(queues.queue(0)[(int) i]));
        }

        System.out.println(values.size());
        Assertions.assertThat(values).containsExactlyInAnyOrder(LongStream.range(0, 400).boxed().toArray(Long[]::new));
    }
}
