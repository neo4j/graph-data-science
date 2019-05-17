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

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.huge.loader.IdMap;
import org.neo4j.graphalgo.core.utils.RandomLongIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@Threads(1)
@Fork(value = 1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class LongIteratorsBenchmark {

    @Param({
            "100000000", // 100M
            "134217728", // 2^27      -- optimal case for random iter
            "134217729"  // 2^27 + 1  -- worst case for random iter
    })
    long size;

    @Benchmark
    public void _01_javaStreamIterator(Blackhole bh) {
        PrimitiveIterator.OfLong iter = LongStream.range(0L, size).iterator();
        while (iter.hasNext()) {
            bh.consume(iter.nextLong());
        }
    }

    @Benchmark
    public void _02_javaStreamRandomIterator(Blackhole bh) {
        Random random = new Random(42L);
        PrimitiveIterator.OfLong iter = LongStream
                .iterate(0L, x -> 1L + (long) random.nextInt((int) size - 1))
                .limit(size)
                .iterator();
        while (iter.hasNext()) {
            bh.consume(iter.nextLong());
        }
    }

    @Benchmark
    public void _03_neoRange(Blackhole bh) {
        PrimitiveLongIterator iter = PrimitiveLongCollections.range(0L, size - 1L);
        while (iter.hasNext()) {
            bh.consume(iter.next());
        }
    }

    @Benchmark
    public void _04_idIterator(Blackhole bh) {
        PrimitiveLongIterator iter = new IdMap.IdIterator(size);
        while (iter.hasNext()) {
            bh.consume(iter.next());
        }
    }

    @Benchmark
    public void _05_switchingIterator(Blackhole bh) {
        PrimitiveLongIterator iter = new IdMap.IdIterator(size);
        iter = new RandomlySwitchingLongIterator(iter, new Random(42L));
        while (iter.hasNext()) {
            bh.consume(iter.next());
        }
    }

    @Benchmark
    public void _06_randomIterator(Blackhole bh) {
        PrimitiveLongIterator iter = new RandomLongIterator(size, new Random(42L));
        while (iter.hasNext()) {
            bh.consume(iter.next());
        }
    }
}
