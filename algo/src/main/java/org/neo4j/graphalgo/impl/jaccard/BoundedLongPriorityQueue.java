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

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

public abstract class BoundedLongPriorityQueue {

    private final int bound;
    private final long[] elements;
    private final double[] priorities;
    private double minValue = Double.NaN;
    private int elementCount = 0;

    BoundedLongPriorityQueue(int bound) {
        this.bound = bound;
        this.elements = new long[bound];
        this.priorities = new double[bound];
    }

    public abstract void offer(long element, double priority);

    public LongStream elements() {
        return elementCount == 0
            ? LongStream.empty()
            : Arrays.stream(elements).limit(elementCount);
    }

    public DoubleStream priorities() {
        return Double.isNaN(minValue)
            ? DoubleStream.empty()
            : Arrays.stream(priorities).limit(elementCount);
    }

    public int count() {
        return elementCount;
    }

    protected void add(long element, double priority) {
        if (elementCount < bound || Double.isNaN(minValue) || priority < minValue) {
            int idx = Arrays.binarySearch(priorities, 0, elementCount, priority);
            idx = (idx < 0) ? -idx : idx + 1;
            int length = bound - idx;
            if (length > 0 && idx < bound) {
                System.arraycopy(priorities, idx - 1, priorities, idx, length);
                System.arraycopy(elements, idx - 1, elements, idx, length);
            }
            priorities[idx - 1] = priority;
            elements[idx - 1] = element;
            if (elementCount < bound) {
                elementCount++;
            }
            minValue = priorities[elementCount - 1];
        }
    }

    public static BoundedLongPriorityQueue max(int bound) {
        return new BoundedLongPriorityQueue(bound) {

            @Override
            public void offer(long element, double priority) {
                add(element, -priority);
            }

            @Override
            public DoubleStream priorities() {
                return super.priorities().map(d -> -d);
            }
        };
    }

    public static BoundedLongPriorityQueue min(int bound) {
        return new BoundedLongPriorityQueue(bound) {

            @Override
            public void offer(long element, double priority) {
                add(element, priority);
            }

        };
    }

}
