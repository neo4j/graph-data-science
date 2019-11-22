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

package org.neo4j.graphalgo.impl.jaccard;

import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfLongArray;

public abstract class BoundedLongLongPriorityQueue {

    public interface Consumer {
        void accept(long element1, long element2, double priority);
    }

    public static MemoryEstimation memoryEstimation(int capacity) {
        return MemoryEstimations.builder(BoundedLongLongPriorityQueue.class)
            .fixed("elements1", sizeOfLongArray(capacity))
            .fixed("elements2", sizeOfLongArray(capacity))
            .fixed("priorities", sizeOfDoubleArray(capacity))
            .build();
    }

    private final int bound;
    private double minValue = Double.NaN;

    final long[] elements1;
    final long[] elements2;
    final double[] priorities;
    int elementCount = 0;

    BoundedLongLongPriorityQueue(int bound) {
        this.bound = bound;
        this.elements1 = new long[bound];
        this.elements2 = new long[bound];
        this.priorities = new double[bound];
    }

    public abstract void offer(long element1, long element2, double priority);

    public abstract void foreach(Consumer consumer);

    public int count() {
        return elementCount;
    }

    protected void add(long element1, long element2, double priority) {
        if (elementCount < bound || Double.isNaN(minValue) || priority < minValue) {
            int idx = Arrays.binarySearch(priorities, 0, elementCount, priority);
            idx = (idx < 0) ? -idx : idx + 1;
            int length = bound - idx;
            if (length > 0 && idx < bound) {
                System.arraycopy(priorities, idx - 1, priorities, idx, length);
                System.arraycopy(elements1, idx - 1, elements1, idx, length);
                System.arraycopy(elements2, idx - 1, elements2, idx, length);
            }
            priorities[idx - 1] = priority;
            elements1[idx - 1] = element1;
            elements2[idx - 1] = element2;
            if (elementCount < bound) {
                elementCount++;
            }
            minValue = priorities[elementCount - 1];
        }
    }

    public LongStream elements1() {
        return elementCount == 0
            ? LongStream.empty()
            : Arrays.stream(elements1).limit(elementCount);
    }

    public LongStream elements2() {
        return elementCount == 0
            ? LongStream.empty()
            : Arrays.stream(elements2).limit(elementCount);
    }

    public DoubleStream priorities() {
        return elementCount == 0
            ? DoubleStream.empty()
            : Arrays.stream(priorities).limit(elementCount);
    }

    public static BoundedLongLongPriorityQueue max(int bound) {
        return new BoundedLongLongPriorityQueue(bound) {

            @Override
            public void offer(long element1, long element2, double priority) {
                add(element1, element2, -priority);
            }

            @Override
            public void foreach(Consumer consumer) {
                for (int i = 0; i < elementCount; i++) {
                    consumer.accept(elements1[i], elements2[i], -priorities[i]);
                }
            }

            @Override
            public DoubleStream priorities() {
                return elementCount == 0
                    ? DoubleStream.empty()
                    : Arrays.stream(priorities).map(d -> -d).limit(elementCount);
            }
        };
    }

    public static BoundedLongLongPriorityQueue min(int bound) {
        return new BoundedLongLongPriorityQueue(bound) {

            @Override
            public void offer(long element1, long element2, double priority) {
                add(element1, element2, priority);
            }

            @Override
            public void foreach(Consumer consumer) {
                for (int i = 0; i < elementCount; i++) {
                    consumer.accept(elements1[i], elements2[i], priorities[i]);
                }
            }
        };
    }
}
