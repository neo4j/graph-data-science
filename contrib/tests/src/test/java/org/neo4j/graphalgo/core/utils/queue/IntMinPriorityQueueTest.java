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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.junit.Test;
import org.neo4j.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public final class IntMinPriorityQueueTest extends RandomizedTest {

    @Test
    public void testIsEmpty() {
        final int capacity = RandomizedTest.between(10, 20);
        final IntPriorityQueue queue = IntPriorityQueue.min(capacity);
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testClear() {
        final int maxSize = RandomizedTest.between(3, 10);
        final IntPriorityQueue queue = IntPriorityQueue.min(maxSize);
        final int iterations = RandomizedTest.between(3, maxSize);
        for (int i = 0; i < iterations; i++) {
            queue.add(i, RandomizedTest.between(1, 5));
        }
        assertEquals(queue.size(), iterations);
        queue.clear();
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testGrowing() {
        final int maxSize = RandomizedTest.between(10, 20);
        final IntPriorityQueue queue = IntPriorityQueue.min(1);
        for (int i = 0; i < maxSize; i++) {
            queue.add(i, RandomizedTest.randomIntBetween(1, 5));
        }
        assertEquals(queue.size(), maxSize);
    }

    @Test
    public void testAdd() {
        final int iterations = RandomizedTest.between(5, 50);
        final IntPriorityQueue queue = IntPriorityQueue.min();
        int min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 0; i < iterations; i++) {
            final double weight = exclusiveDouble(0d, 100d);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            queue.add(i, weight);
            assertEquals(queue.top(), min);
        }
    }

    @Test
    public void testAddAndPop() {
        final IntPriorityQueue queue = IntPriorityQueue.min();
        final List<Pair<Integer, Double>> elements = new ArrayList<>();

        final int iterations = RandomizedTest.between(5, 50);
        int min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(0d, 100d);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            queue.add(i, weight);
            assertEquals(queue.top(), min);
            elements.add(Pair.of(i, weight));
        }

        // PQ isn't stable for duplicate elements, so we have to
        // test those with non strict ordering requirements
        final Map<Double, Set<Integer>> byWeight = elements
                .stream()
                .collect(Collectors.groupingBy(
                        Pair::other,
                        Collectors.mapping(Pair::first, Collectors.toSet())));
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
    public void testUpdateDecreasing() {
        final IntPriorityQueue queue = IntPriorityQueue.min();

        final int iterations = RandomizedTest.between(5, 50);
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(50d, 100d);
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
    public void testUpdateIncreasing() {
        final IntPriorityQueue queue = IntPriorityQueue.min();
        final int iterations = RandomizedTest.between(5, 50);
        for (int i = 1; i <= iterations; i++) {
            queue.add(i, exclusiveDouble(50d, 100d));
        }

        final int top = queue.top();
        for (int i = iterations + 1; i < iterations + 10; i++) {
            queue.addCost(i, 1d);
            queue.update(i);
            assertEquals(top, queue.top());
        }
    }

    @Test
    public void testUpdateNotExisting() {
        final IntPriorityQueue queue = IntPriorityQueue.min();

        final int iterations = RandomizedTest.between(5, 50);
        double maxWeight = Double.NEGATIVE_INFINITY;
        for (int i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(50d, 100d);
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
        return RandomizedTest.biasedDoubleBetween(
                Math.nextUp(exclusiveMin),
                Math.nextDown(exclusiveMax));
    }
}
