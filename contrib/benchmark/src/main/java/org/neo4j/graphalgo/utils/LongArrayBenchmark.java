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

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;
import org.neo4j.unsafe.impl.batchimport.cache.DynamicLongArray;
import org.neo4j.unsafe.impl.batchimport.cache.OffHeapLongArray;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class LongArrayBenchmark {

    @Benchmark
    public long primitive_get(LongArrays arrays) {
        final int size = arrays.size;
        final long[] array = arrays.primitive;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array[i];
        }
        return res;
    }

    @Benchmark
    public long[] primitive_set(LongArrays arrays) {
        final int size = arrays.size;
        final long[] array = arrays.primitive;
        long[] result = new long[size];
        for (int i = 0; i < size; i++) {
            if (array[i] >= 0) {
                result[i] = array[i];
            }
        }
        return result;
    }

    @Benchmark
    public long paged_get(LongArrays arrays) {
        final int size = arrays.size;
        final HugeLongArray array = arrays.paged;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public HugeLongArray paged_set(LongArrays arrays) {
        return LongArrays.createPaged(arrays.primitive);
    }

    @Benchmark
    public long huge_get(LongArrays arrays) {
        final int size = arrays.size;
        final HugeLongArray array = arrays.huge;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public HugeLongArray huge_set(LongArrays arrays) {
        return LongArrays.createHuge(arrays.primitive);
    }

    @Benchmark
    public long sparse_get(LongArrays arrays) {
        final int size = arrays.size;
        final SparseLongArray array = arrays.sparse;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public SparseLongArray sparse_set(LongArrays arrays) {
        return LongArrays.createSparse(arrays.primitive);
    }

    @Benchmark
    public long offHeap_get(LongArrays arrays) {
        final int size = arrays.size;
        final OffHeapLongArray array = arrays.offHeap;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public OffHeapLongArray offHeap_set(LongArrays arrays) {
        return LongArrays.createOffHeap(arrays.primitive);
    }

    @Benchmark
    public long chunked_get(LongArrays arrays) {
        final int size = arrays.size;
        final DynamicLongArray array = arrays.chunked;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public DynamicLongArray chunked_set(LongArrays arrays) {
        return LongArrays.createChunked(arrays.primitive);
    }
}
