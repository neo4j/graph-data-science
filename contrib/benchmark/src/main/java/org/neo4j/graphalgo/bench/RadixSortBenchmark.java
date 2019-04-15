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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.core.huge.loader.RadixSort;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Threads(1)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RadixSortBenchmark {

    @Param({"10000", "100000", "1000000"})
    int size;

    long[] data;
    long[] copy;
    int[] histogram;

    @Setup
    public void setup() {
        Random random = new Random(1337L);
        int size = this.size;
        List<Integer> values = IntStream
                .range(0, size)
                .map(i -> random.nextInt(16) + (i << 4))
                .boxed()
                .collect(Collectors.toList());
        Collections.shuffle(values, random);
        long[] data = values.stream().mapToLong(i -> i).toArray();

        this.data = data;
        this.copy = RadixSort.newCopy(data);
        this.histogram = RadixSort.newHistogram(0);
    }

    @Benchmark
    public long[] juArraysSort() {
        long[] data = this.data.clone();
        Arrays.sort(data);
        return data;
    }

    @Benchmark
    public long[] radixSort1() {
        long[] data = this.data.clone();
        RadixSort.radixSort(data, copy, histogram, data.length);
        return data;
    }

    @Benchmark
    public long[] radixSort2() {
        long[] data = this.data.clone();
        RadixSort.radixSort2(data, copy, histogram, data.length);
        return data;
    }
}
