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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.beta.pregel.PrimitiveAsyncDoubleQueues.COMPACT_THRESHOLD;

class PrimitiveAsyncDoubleQueuesTest extends PrimitiveDoubleQueuesTest {

    @Override
    PrimitiveDoubleQueues getQueue(long nodeCount, int initialCapacity) {
        return PrimitiveAsyncDoubleQueues.of(nodeCount, initialCapacity, AllocationTracker.empty());
    }

    @Test
    void isEmpty() {
        var queues = PrimitiveAsyncDoubleQueues.of(1, AllocationTracker.empty());
        assertThat(queues.isEmpty(0)).isTrue();

        queues.push(0, 42);
        assertThat(queues.isEmpty(0)).isFalse();

        queues.pop(0);
        assertThat(queues.isEmpty(0)).isTrue();
    }

    @Test
    void isEmptyFullArray() {
        var queues = PrimitiveAsyncDoubleQueues.of(1, AllocationTracker.empty());

        for (int i = 0; i < 42; i++) {
            queues.push(0, i);
        }

        for (int i = 0; i < 42; i++) {
            queues.pop(0);
        }

        assertThat(queues.isEmpty(0)).isTrue();
    }

    @Test
    void isEmptyWorksAfterGrowing() {
        var initialCapacity = 50;
        var insertedElements = initialCapacity + 10;
        var queues = PrimitiveAsyncDoubleQueues.of(1, initialCapacity, AllocationTracker.empty());

        for (int i = 0; i < insertedElements; i++) {
            queues.push(0, 42);
        }

        var popCount = 0;
        while (!queues.isEmpty(0)) {
            popCount++;
            assertThat(queues.pop(0)).isEqualTo(42);
        }

        assertThat(popCount).isEqualTo(insertedElements);
    }

    @Test
    void pop() {
        var queues = PrimitiveAsyncDoubleQueues.of(1, AllocationTracker.empty());
        queues.push(0, 42.0D);
        assertThat(queues.pop(0)).isEqualTo(42.0D);

        queues.push(0, 42.0D);
        queues.push(0, 84.0D);
        assertThat(queues.pop(0)).isEqualTo(42.0D);
        assertThat(queues.pop(0)).isEqualTo(84.0D);
    }

    @Test
    void compactEmptyQueue() {
        var queues = PrimitiveAsyncDoubleQueues.of(1, 50, AllocationTracker.empty());

        var minFillSize = Math.ceil(50 * COMPACT_THRESHOLD);
        for (int i = 0; i < minFillSize; i++) {
            queues.push(0, 42);
            queues.pop(0);
        }

        queues.compact();

        assertThat(queues.isEmpty(0)).isTrue();
        assertThat(queues.head(0)).isEqualTo(0);
        assertThat(queues.tail(0)).isEqualTo(0);
        assertThat(queues.queue(0)).containsOnly(Double.NaN);
    }

    @Test
    void compactNoneEmptyQueue() {
        var queues = PrimitiveAsyncDoubleQueues.of(1, 50, AllocationTracker.empty());

        var minFillSize = Math.ceil(50*COMPACT_THRESHOLD);

        // Fill the queue and consume it
        for (int i = 0; i < minFillSize; i++) {
            queues.push(0, 42);
            queues.pop(0);
        }

        // Fill the queue again
        var fillSize = 5;
        for (int i = 0; i < fillSize; i++) {
            queues.push(0, i);
        }

        queues.compact();

        assertThat(queues.isEmpty(0)).isFalse();
        assertThat(queues.head(0)).isEqualTo(0);
        assertThat(queues.tail(0)).isEqualTo(fillSize);

        for (int i = 0; i < fillSize; i++) {
            assertThat(queues.pop(0)).isEqualTo(i);
        }
    }

    @Test
    void compactOnlyIfAboveThresholdNoneEmptyQueue() {
        var queues = PrimitiveAsyncDoubleQueues.of(1, 50, AllocationTracker.empty());

        var compactThreshold = Math.round(Math.floor(50*COMPACT_THRESHOLD)) - 1;

        // Fill the queue and consume it
        for (int i = 0; i < compactThreshold; i++) {
            queues.push(0, 42);
            queues.pop(0);
        }

        // Fill the queue again
        var fillSize = 5;
        for (int i = 0; i < fillSize; i++) {
            queues.push(0, i);
        }

        queues.compact();

        assertThat(queues.isEmpty(0)).isFalse();
        assertThat(queues.head(0)).isEqualTo(compactThreshold);
        assertThat(queues.tail(0)).isEqualTo(compactThreshold + fillSize);

        for (int i = 0; i < fillSize; i++) {
            assertThat(queues.pop(0)).isEqualTo(i);
        }
    }
}
