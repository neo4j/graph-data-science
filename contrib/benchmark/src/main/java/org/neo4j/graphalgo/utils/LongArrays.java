/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.utils;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.NewHugeArrays;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.unsafe.impl.batchimport.cache.ChunkedHeapFactory;
import org.neo4j.unsafe.impl.batchimport.cache.DynamicLongArray;
import org.neo4j.unsafe.impl.batchimport.cache.OffHeapLongArray;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Arrays;
import java.util.Random;

@State(Scope.Benchmark)
public class LongArrays {

    public enum Distribution {
        uniform, packed;
    }

    @Param({"100000", "10000000"})
    int size;

    @Param({"0.1", "0.5", "0.9", "0.999"})
    double sparseness;

    @Param({"uniform", "packed"})
    Distribution distribution;

    long[] primitive;
    HugeLongArray paged;
    HugeLongArray huge;
    SparseLongArray sparse;
    OffHeapLongArray offHeap;
    DynamicLongArray chunked;

    @Setup
    public void setup() {
        primitive = createPrimitive(size, sparseness, distribution);
        paged = createPaged(primitive);
        huge = createHuge(primitive);
        sparse = createSparse(primitive);
        offHeap = createOffHeap(primitive);
        chunked = createChunked(primitive);
    }

    private static long[] createPrimitive(
            int size,
            double sparseness,
            Distribution distribution) {
        Random rand = new Random(0);
        long[] array = new long[size];
        if (distribution == Distribution.packed) {
            int blockSize = (int) Math.round(size * (1.0 - sparseness));
            int maxIndex = size - blockSize;
            int startIndex = rand.nextInt(maxIndex);
            Arrays.fill(array, -1L);
            for (int i = 0; i < blockSize; i++) {
                array[i + startIndex] = randomLong(rand);
            }
        } else {
            for (int i = 0; i < size; ++i) {
                if (rand.nextDouble() >= sparseness) {
                    array[i] = randomLong(rand);
                } else {
                    array[i] = -1L;
                }
            }
        }
        return array;
    }

    static HugeLongArray createPaged(long[] values) {
        final HugeLongArray array = NewHugeArrays.newPagedArray(values.length, AllocationTracker.EMPTY);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    static HugeLongArray createHuge(long[] values) {
        final HugeLongArray array = HugeLongArray.newArray(values.length, AllocationTracker.EMPTY);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    static SparseLongArray createSparse(long[] values) {
        final SparseLongArray array = SparseLongArray.newArray(values.length, AllocationTracker.EMPTY);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    static OffHeapLongArray createOffHeap(long[] values) {
        final OffHeapLongArray array = new OffHeapLongArray(values.length, -1L, 0, NoMemoryTracker.INSTANCE);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    static DynamicLongArray createChunked(long[] values) {
        final DynamicLongArray array = ChunkedHeapFactory.newArray(values.length, -1L);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    private static long randomLong(Random random) {
        long v;
        do {
            v = random.nextLong();
        } while (v < 0);
        return v;
    }
}
