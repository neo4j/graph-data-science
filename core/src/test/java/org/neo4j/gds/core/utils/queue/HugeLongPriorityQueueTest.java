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
package org.neo4j.gds.core.utils.queue;

import io.qala.datagen.RandomShortApi;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static io.qala.datagen.RandomShortApi.integer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class HugeLongPriorityQueueTest {

    @Test
    void testIsEmpty() {
        var capacity = integer(10, 20);
        var queue = HugeLongPriorityQueue.min(capacity);
        assertThat(queue.size()).isEqualTo(0L);
        assertThatThrownBy(() -> queue.top()
        ).hasMessageContaining("empty");
    }

    @Test
    void testClear() {
        var maxSize = integer(3, 10);
        var queue = HugeLongPriorityQueue.min(maxSize);
        var count = integer(3, maxSize);
        for (long element = 0; element < count; element++) {
            queue.add(element, integer(1, 5));
        }
        assertThat(queue.size()).isEqualTo(count);
        queue.clear();
        assertThat(queue.size()).isEqualTo(0L);
    }

    @Test
    void testAdd() {
        var size = 50;
        var count = integer(5, size);
        var queue = HugeLongPriorityQueue.min(size);
        var minElement = -1L;
        var minCost = Double.POSITIVE_INFINITY;

        for (long key = 0; key < count; key++) {
            double weight = exclusiveDouble(0D, 100D);
            if (weight < minCost) {
                minCost = weight;
                minElement = key;
            }
            queue.add(key, weight);
            assertThat(queue.top()).isEqualTo(minElement);
        }
    }

    @Test
    void testAddAndPop() {
        var capacity = 50;
        var queue = HugeLongPriorityQueue.min(capacity);
        var elements = new ArrayList<Pair<Long, Double>>();
        var count = integer(5, capacity);
        var minElement = -1L;
        var minCost = Double.POSITIVE_INFINITY;

        for (long element = 0; element < count; element++) {
            var weight = exclusiveDouble(0D, 100D);
            if (weight < minCost) {
                minCost = weight;
                minElement = element;
            }
            queue.add(element, weight);
            assertThat(queue.top()).isEqualTo(minElement);
            elements.add(Tuples.pair(element, weight));
        }

        // PQ isn't stable for duplicate elements, so we have to
        // test those with non strict ordering requirements
        var byCost = elements
            .stream()
            .collect(Collectors.groupingBy(
                Pair::getTwo,
                Collectors.mapping(Pair::getOne, Collectors.toSet())
            ));
        var costGroups = byCost
            .keySet()
            .stream()
            .sorted()
            .collect(Collectors.toList());

        for (var cost : costGroups) {
            var allowedElements = byCost.get(cost);
            while (!allowedElements.isEmpty()) {
                long item = queue.pop();
                assertThat(item).isIn(allowedElements);
                allowedElements.remove(item);
            }
        }

        assertThat(queue.isEmpty()).isTrue();
    }

    @Test
    void testUpdateDecreasing() {
        var capacity = 50;
        var queue = HugeLongPriorityQueue.min(capacity);

        var count = integer(5, capacity);
        var minCost = Double.POSITIVE_INFINITY;
        for (long element = 0; element < count; element++) {
            double weight = exclusiveDouble(50D, 100D);
            if (weight < minCost) {
                minCost = weight;
            }
            queue.add(element, weight);
        }

        for (long element = count - 1; element >= 0; element--) {
            minCost = Math.nextDown(minCost);
            queue.set(element, minCost);
            assertThat(element).isEqualTo(queue.top());
        }
    }

    @Test
    void testUpdateIncreasing() {
        var capacity = 50;
        var queue = HugeLongPriorityQueue.min(capacity);
        int count = integer(5, capacity);
        double maxCost = Double.NEGATIVE_INFINITY;

        for (long element = 0; element < count; element++) {
            var weight = exclusiveDouble(50D, 100D);
            if (weight > maxCost) {
                maxCost = weight;
            }
            queue.add(element, weight);
        }

        var top = queue.top();
        for (var element = count - 1; element >= 0; element--) {
            if (element == top) {
                continue;
            }
            maxCost = Math.nextUp(maxCost);
            queue.set(element, maxCost);
            assertThat(top).isEqualTo(queue.top());
        }
    }

    @Test
    void testAddAndSetDifferentPrio() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(2);
        priorityQueue.add(1, 1);
        priorityQueue.set(1, 2);
        assertThat(priorityQueue.size()).isEqualTo(1);
    }

    @Test
    void testAddAndSetSamePrio() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(2);
        priorityQueue.add(1, 1);
        priorityQueue.set(1, 1);
        assertThat(priorityQueue.size()).isEqualTo(1);
    }

    @Test
    void testSetZeroCost() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(2);
        priorityQueue.set(1, 0);
        priorityQueue.set(1, 0);
        assertThat(priorityQueue.size()).isEqualTo(1);
    }

    @Test
    void testMapIndex() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(11);
        for (int i = 0; i < 10; ++i) {
            priorityQueue.add(i, 10 - i);
        }
        for (int i = 0; i < 10; ++i) {
            var ith = priorityQueue.getIth(i);
            assertThat(priorityQueue.findElementPosition(ith)).isEqualTo(i + 1);
        }
        assertThat(priorityQueue.findElementPosition(10)).isEqualTo(0);
    }

    @Test
    void shouldKeepCostAfterDeleting() {
        //This test verifies that after deleting an element .cost returns its last value
        //If you change HLPQ so that this test fails, please revert the changes in
        //https://github.com/neo-technology/graph-analytics/pull/7360 to use the 'costToParent' array again.
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(11);
        for (int i = 0; i < 10; ++i) {
            priorityQueue.add(i, 10 - i);
        }
        assertThat(priorityQueue.cost(9)).isEqualTo(1);
        for (int i = 0; i < 10; ++i) {
            priorityQueue.pop();
        }
        for (int i = 0; i < 9; ++i) {
            priorityQueue.add(i, 10 - i);
        }
        assertThat(priorityQueue.containsElement(9)).isFalse();
        assertThat(priorityQueue.cost(9)).isEqualTo(1);
    }

    private double exclusiveDouble(double exclusiveMin, double exclusiveMax) {
        return RandomShortApi.Double(Math.nextUp(exclusiveMin), exclusiveMax);
    }
}
