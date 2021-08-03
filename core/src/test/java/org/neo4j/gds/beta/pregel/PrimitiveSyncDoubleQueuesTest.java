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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class PrimitiveSyncDoubleQueuesTest extends PrimitiveDoubleQueuesTest {

    @Override
    PrimitiveSyncDoubleQueues getQueue(long nodeCount, int initialCapacity) {
        return PrimitiveSyncDoubleQueues.of(nodeCount, initialCapacity, AllocationTracker.empty());
    }

    @Test
    void swapQueues() {
        var initialCapacity = 42;
        var queue = getQueue(1, initialCapacity);

        var iterations = 10;

        for (int iteration = 0; iteration < iterations; iteration++) {
            for (int i = 0; i < 2 * initialCapacity; i++) {
                queue.push(0, i * iteration);
            }

            var expectedSum = IntStream.range(0, 2 * initialCapacity).sum() * iteration;
            var actualSum = Arrays.stream(queue.queue(0)).sum();
            assertThat(actualSum).isEqualTo(expectedSum);

            queue.swapQueues();
        }
    }

    @Nested
    class IteratorTest {

        @Test
        void iterate() {
            var initialCapacity = 42;
            var queue = getQueue(1, initialCapacity);

            for (int i = 0; i < 2 * initialCapacity; i++) {
                queue.push(0, i);
            }

            queue.swapQueues();

            var iterator = new PrimitiveSyncDoubleQueues.Iterator();
            queue.initIterator(iterator, 0);

            var sum = 0D;
            while (iterator.hasNext()) {
                sum += iterator.nextDouble();
            }

            assertThat(sum).isEqualTo(IntStream.range(0, 2 * initialCapacity).sum());
        }

        @Test
        void iterateEmptyQueue() {
            var initialCapacity = 42;
            var queue = getQueue(1, initialCapacity);

            queue.swapQueues();

            var iterator = new PrimitiveSyncDoubleQueues.Iterator();
            queue.initIterator(iterator, 0);

            assertThat(iterator.hasNext()).isFalse();
        }
    }
}
