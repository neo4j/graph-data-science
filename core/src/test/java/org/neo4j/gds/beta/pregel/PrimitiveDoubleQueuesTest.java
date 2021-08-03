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
package org.neo4j.gds.beta.pregel;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.Phaser;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

abstract class PrimitiveDoubleQueuesTest {

    abstract PrimitiveDoubleQueues getQueue(long nodeCount, int initialCapacity);

    @Test
    void growQueueArray() {
        var queues = getQueue(1337, 42);

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

    @RepeatedTest(100)
    void parallelPush() {
        var queues = getQueue(1, 42);
        var concurrency = 4;
        var phaser = new Phaser(concurrency + 1);

        IntStream.range(0, concurrency).mapToObj((taskOffset) -> (Runnable) () -> {
            phaser.arriveAndAwaitAdvance();
            for (int i = 0; i < 100; i++) {
                queues.push(0, i + taskOffset * 100);
            }
            phaser.arriveAndAwaitAdvance();
        }).forEach(task -> new Thread(task).start());

        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndAwaitAdvance();

        var values = new ArrayList<Long>();
        for (long i = 0; i < queues.tail(0); i++) {
            values.add(Math.round(queues.queue(0)[(int) i]));
        }

        assertThat(values)
            .containsExactlyInAnyOrder(LongStream.range(0, concurrency * 100).boxed().toArray(Long[]::new));
    }

}
