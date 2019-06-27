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
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.NewHugeArrays;
import org.neo4j.graphalgo.core.huge.loader.SparseNodeMapping;
import org.neo4j.unsafe.impl.batchimport.cache.ChunkedHeapFactory;
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
    public long huge_paged_get(LongArrays arrays) {
        final int size = arrays.size;
        final HugeLongArray array = arrays.paged;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public long huge_paged_get_cursor(LongArrays arrays) {
        final HugeLongArray array = arrays.paged;
        HugeCursor<long[]> cursor = array.cursor(array.newCursor());
        long res = 0;
        long[] block;
        int limit;
        while (cursor.next()) {
            block = cursor.array;
            limit = cursor.limit;
            for (int i = cursor.offset; i < limit; ++i) {
                res += block[i];
            }
        }
        return res;
    }

    @Benchmark
    public HugeLongArray huge_paged_set(LongArrays arrays) {
        long[] values = arrays.primitive;
        final HugeLongArray array = NewHugeArrays.newPagedArray(values.length, AllocationTracker.EMPTY);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    @Benchmark
    public HugeLongArray huge_paged_set_cursor(LongArrays arrays) {
        long[] values = arrays.primitive;
        final HugeLongArray array = NewHugeArrays.newPagedArray(values.length, AllocationTracker.EMPTY);
        HugeCursor<long[]> cursor = array.cursor(array.newCursor());
        long[] block;
        int limit, offset, idx;
        while (cursor.next()) {
            block = cursor.array;
            limit = cursor.limit;
            offset = cursor.offset;
            idx = (int) cursor.base;
            while (offset < limit) {
             long value = values[idx++];
                if (value >= 0L) {
                    block[offset] = value;
                }
                ++offset;
            }
        }
        return array;
    }

    @Benchmark
    public long huge_single_get(LongArrays arrays) {
        final int size = arrays.size;
        final HugeLongArray array = arrays.single;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public long huge_single_get_cursor(LongArrays arrays) {
        final HugeLongArray array = arrays.single;
        HugeCursor<long[]> cursor = array.cursor(array.newCursor());
        long res = 0;
        long[] block;
        int limit;
        while (cursor.next()) {
            block = cursor.array;
            limit = cursor.limit;
            for (int i = cursor.offset; i < limit; ++i) {
                res += block[i];
            }
        }
        return res;
    }

    @Benchmark
    public HugeLongArray huge_single_set(LongArrays arrays) {
        long[] values = arrays.primitive;
        final HugeLongArray array = HugeLongArray.newArray(values.length, AllocationTracker.EMPTY);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }

    @Benchmark
    public HugeLongArray huge_single_set_cursor(LongArrays arrays) {
        long[] values = arrays.primitive;
        final HugeLongArray array = HugeLongArray.newArray(values.length, AllocationTracker.EMPTY);
        HugeCursor<long[]> cursor = array.cursor(array.newCursor());
        long[] block;
        int limit, offset, idx;
        while (cursor.next()) {
            block = cursor.array;
            limit = cursor.limit;
            offset = cursor.offset;
            idx = (int) cursor.base;
            while (offset < limit) {
             long value = values[idx++];
                if (value >= 0L) {
                    block[offset] = value;
                }
                ++offset;
            }
        }
        return array;
    }

    @Benchmark
    public long sparse_get(LongArrays arrays) {
        final int size = arrays.size;
        final SparseNodeMapping array = arrays.sparse;
        long res = 0;
        for (int i = 0; i < size; i++) {
            res += array.get(i);
        }
        return res;
    }

    @Benchmark
    public SparseNodeMapping sparse_set(LongArrays arrays) {
        long[] values = arrays.primitive;
        final SparseNodeMapping.Builder array = SparseNodeMapping.Builder.create(values.length, AllocationTracker.EMPTY);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array.build();
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
        long[] values = arrays.primitive;
        final OffHeapLongArray array = new OffHeapLongArray(values.length, -1L, 0, NoMemoryTracker.INSTANCE);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
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
        long[] values = arrays.primitive;
        final DynamicLongArray array = ChunkedHeapFactory.newArray(values.length, -1L);
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            if (value >= 0) {
                array.set(i, value);
            }
        }
        return array;
    }
}
