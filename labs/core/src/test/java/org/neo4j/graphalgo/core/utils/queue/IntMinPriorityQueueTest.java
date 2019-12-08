/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.queue;

import io.qala.datagen.RandomShortApi;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.qala.datagen.RandomShortApi.integer;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class IntMinPriorityQueueTest {

    @Test
    void testIsEmpty() {
        final int capacity = integer(10, 20);
        final IntPriorityQueue queue = IntPriorityQueue.min(capacity);
        assertEquals(queue.size(), 0);
    }

    @Test
    void testClear() {
        final int maxSize = integer(3, 10);
        final IntPriorityQueue queue = IntPriorityQueue.min(maxSize);
        final int iterations = integer(3, maxSize);
        for (int i = 0; i < iterations; i++) {
            queue.add(i, integer(1, 5));
        }
        assertEquals(queue.size(), iterations);
        queue.clear();
        assertEquals(queue.size(), 0);
    }

    @Test
    void testGrowing() {
        final int maxSize = integer(10, 20);
        final IntPriorityQueue queue = IntPriorityQueue.min(1);
        for (int i = 0; i < maxSize; i++) {
            queue.add(i, integer(1, 5));
        }
        assertEquals(queue.size(), maxSize);
    }

    @Test
    void testAdd() {
        final int iterations = integer(5, 50);
        final IntPriorityQueue queue = IntPriorityQueue.min();
        int min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 0; i < iterations; i++) {
            final double weight = exclusiveDouble(0D, 100D);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            queue.add(i, weight);
            assertEquals(queue.top(), min);
        }
    }

    @Test
    void testAddAndPop() {
        final IntPriorityQueue queue = IntPriorityQueue.min();
        final List<Pair<Integer, Double>> elements = new ArrayList<>();

        final int iterations = integer(5, 50);
        int min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(0D, 100D);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            queue.add(i, weight);
            assertEquals(queue.top(), min);
            elements.add(Tuples.pair(i, weight));
        }

        // PQ isn't stable for duplicate elements, so we have to
        // test those with non strict ordering requirements
        final Map<Double, Set<Integer>> byWeight = elements
                .stream()
                .collect(Collectors.groupingBy(
                        Pair::getTwo,
                        Collectors.mapping(Pair::getOne, Collectors.toSet())));
        final List<Double> weightGroups = byWeight
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        for (Double weight : weightGroups) {
            final Set<Integer> allowedIds = byWeight.get(weight);
            while (!allowedIds.isEmpty()) {
                final int item = queue.pop();
                assertThat(allowedIds, hasItem(item));
                allowedIds.remove(item);
            }
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    void testUpdateDecreasing() {
        final IntPriorityQueue queue = IntPriorityQueue.min();

        final int iterations = integer(5, 50);
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(50D, 100D);
            if (weight < minWeight) {
                minWeight = weight;
            }
            queue.add(i, weight);
        }

        for (int i = iterations; i >= 1; i--) {
            minWeight = Math.nextDown(minWeight);
            queue.addCost(i, minWeight);
            queue.update(i);
            assertEquals(i, queue.top());
        }
    }

    @Test
    void testUpdateIncreasing() {
        final IntPriorityQueue queue = IntPriorityQueue.min();
        final int iterations = integer(5, 50);
        for (int i = 1; i <= iterations; i++) {
            queue.add(i, exclusiveDouble(50D, 100D));
        }

        final int top = queue.top();
        for (int i = iterations + 1; i < iterations + 10; i++) {
            queue.addCost(i, 1D);
            queue.update(i);
            assertEquals(top, queue.top());
        }
    }

    @Test
    void testUpdateNotExisting() {
        final IntPriorityQueue queue = IntPriorityQueue.min();

        final int iterations = integer(5, 50);
        double maxWeight = Double.NEGATIVE_INFINITY;
        for (int i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(50D, 100D);
            if (weight > maxWeight) {
                maxWeight = weight;
            }
            queue.add(i, weight);
        }

        int top = queue.top();
        for (int i = iterations; i >= 1; i--) {
            if (i == top) {
                continue;
            }
            maxWeight = Math.nextUp(maxWeight);
            queue.addCost(i, maxWeight);
            queue.update(i);
            assertEquals(top, queue.top());
        }
    }

    private double exclusiveDouble(
            final double exclusiveMin,
            final double exclusiveMax) {
        return RandomShortApi.Double(Math.nextUp(exclusiveMin), exclusiveMax);
    }
}
