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

final class LongPriorityQueueTest {

    @Test
    void testIsEmpty() {
        int capacity = integer(10, 20);
        LongPriorityQueue queue = LongPriorityQueue.min(capacity);
        assertEquals(queue.size(), 0);
    }

    @Test
    void testClear() {
        int maxSize = integer(3, 10);
        LongPriorityQueue queue = LongPriorityQueue.min(maxSize);
        int iterations = integer(3, maxSize);
        for (int i = 0; i < iterations; i++) {
            queue.add(i, integer(1, 5));
        }
        assertEquals(queue.size(), iterations);
        queue.clear();
        assertEquals(queue.size(), 0);
    }

    @Test
    void testGrowing() {
        int maxSize = integer(10, 20);
        LongPriorityQueue queue = LongPriorityQueue.min(1);
        for (int i = 0; i < maxSize; i++) {
            queue.add(i, integer(1, 5));
        }
        assertEquals(queue.size(), maxSize);
    }

    @Test
    void testAdd() {
        int iterations = integer(5, 50);
        LongPriorityQueue queue = LongPriorityQueue.min();
        int min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 0; i < iterations; i++) {
            double weight = exclusiveDouble(0D, 100D);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            assertEquals(queue.add(i, weight), min);
        }
    }

    @Test
    void testAddAndPop() {
        LongPriorityQueue queue = LongPriorityQueue.min();
        List<Pair<Long, Double>> elements = new ArrayList<>();

        int iterations = integer(5, 50);
        long min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (long i = 1; i <= iterations; i++) {
            double weight = exclusiveDouble(0D, 100D);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            assertEquals(queue.add(i, weight), min);
            elements.add(Tuples.pair(i, weight));
        }

        // PQ isn't stable for duplicate elements, so we have to
        // test those with non strict ordering requirements
        Map<Double, Set<Long>> byWeight = elements
                .stream()
                .collect(Collectors.groupingBy(
                        Pair::getTwo,
                        Collectors.mapping(Pair::getOne, Collectors.toSet())));
        List<Double> weightGroups = byWeight
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        for (Double weight : weightGroups) {
            Set<Long> allowedIds = byWeight.get(weight);
            while (!allowedIds.isEmpty()) {
                long item = queue.pop();
                assertThat(allowedIds, hasItem(item));
                allowedIds.remove(item);
            }
        }

        assertTrue(queue.isEmpty());
    }

    private double exclusiveDouble(
            double exclusiveMin,
            double exclusiveMax
    ) {
        return RandomShortApi.Double(Math.nextUp(exclusiveMin), exclusiveMax);
    }
}
