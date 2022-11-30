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
import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.stream.Collectors;

import static io.qala.datagen.RandomShortApi.integer;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HugeLongPriorityQueueTest {

    @Test
    void testIsEmpty() {
        var capacity = integer(10, 20);
        var queue = HugeLongPriorityQueue.min(capacity);
        assertEquals(queue.size(), 0);
    }

    @Test
    void testClear() {
        var maxSize = integer(3, 10);
        var queue = HugeLongPriorityQueue.min(maxSize);
        var count = integer(3, maxSize);
        for (long element = 0; element < count; element++) {
            queue.add(element, integer(1, 5));
        }
        assertEquals(queue.size(), count);
        queue.clear();
        assertEquals(queue.size(), 0);
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
            assertEquals(queue.top(), minElement);
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
            assertEquals(queue.top(), minElement);
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
                assertThat(allowedElements, hasItem(item));
                allowedElements.remove(item);
            }
        }

        assertTrue(queue.isEmpty());
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
            assertEquals(element, queue.top());
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
            assertEquals(top, queue.top());
        }
    }

    @Test
    void testAddAndSetDifferentPrio() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(2);
        priorityQueue.add(1, 1);
        priorityQueue.set(1, 2);
        Assertions.assertThat(priorityQueue.size()).isEqualTo(1);
    }

    @Test
    void testAddAndSetSamePrio() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(2);
        priorityQueue.add(1, 1);
        priorityQueue.set(1, 1);
        Assertions.assertThat(priorityQueue.size()).isEqualTo(1);
    }

    @Test
    void testSetZeroCost() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(2);
        priorityQueue.set(1, 0);
        priorityQueue.set(1, 0);
        Assertions.assertThat(priorityQueue.size()).isEqualTo(1);
    }

    @Test
    void testMapIndex() {
        HugeLongPriorityQueue priorityQueue = HugeLongPriorityQueue.min(11);
        for (int i = 0; i < 10; ++i) {
            priorityQueue.add(i, 10 - i);
        }
        for (int i = 0; i < 10; ++i) {
            var ith = priorityQueue.getIth(i);
            assertEquals(priorityQueue.findElementPosition(ith), i + 1);
        }
        assertEquals(priorityQueue.findElementPosition(10), 0);
    }

    private double exclusiveDouble(double exclusiveMin, double exclusiveMax) {
        return RandomShortApi.Double(Math.nextUp(exclusiveMin), exclusiveMax);
    }
}
