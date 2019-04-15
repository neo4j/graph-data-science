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

import com.carrotsearch.hppc.LongHashSet;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 1000)
@Measurement(iterations = 10000, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class IntersectionBenchmark {

    // @Param({"", "Person"}) String label;

    private static long[][][] data = {
            {{1,2,4},{1,3,5},{1}},
            {{1,2,4},{1,2,5},{2}},
            {{1,2,4},{1,2,4},{3}},
            {{1,2,4},{1,2,4,5},{3}},
            {{1,2,4,5},{1,4,5},{3}},
            {{},{},{0}},
            {{},{1},{0}},
            {{1},{},{0}},
            {{1},{1},{1}},
            {{1},{0},{0}},
            {{0},{1},{0}},
            {{1,2,4,5},{1,5},{2}},
            {{1,2,4,5},{1,2},{2}},
            {{1,2,4,5},{1,4},{2}},
            {{1,2,4,5},{2,4},{2}},
            {{1,2,4,5},{2,5},{2}},
            {{1,2,4},{0,3,5},{0}},
            generate(10_000, 655)
            // todo generate large sets
    };

    private static long[][] generate(int size, int overlap) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        LongHashSet overlapSet = new LongHashSet(overlap);
        generateUniqueRandomSet(overlap, random, overlapSet, overlapSet);

        LongHashSet first = new LongHashSet(size);
        generateUniqueRandomSet(size, random, overlapSet, first);
        first.addAll(overlapSet);

        LongHashSet second = new LongHashSet(size);
        generateUniqueRandomSet(size, random, first, second);
        second.addAll(overlapSet);

        return new long[][] {first.toArray(), second.toArray(),new long[] {overlap}};
    }

    private static void generateUniqueRandomSet(int size, ThreadLocalRandom random, LongHashSet exclude, LongHashSet set) {
        long rnd;
        for (int i = 0; i < size; i++) {
            do {
                rnd = random.nextLong();
            } while (set.contains(rnd) || exclude.contains(rnd));
            set.add(rnd);
        }
    }


    @Benchmark
    public void intersection(Blackhole bh) throws Exception {
        for (long[][] row : data) {
            bh.consume(Intersections.intersection(LongHashSet.from(row[0]),LongHashSet.from(row[1])));

        }
    }

    @Benchmark
    public void intersection2(Blackhole bh) throws Exception {
        for (long[][] row : data) {
            bh.consume(Intersections.intersection2(row[0],row[1]));
        }

    }

    @Benchmark
    public void intersection3(Blackhole bh) throws Exception {
        for (long[][] row : data) {
            bh.consume(Intersections.intersection3(row[0],row[1]));
        }
    }

    @Benchmark
    public void intersection4(Blackhole bh) throws Exception {
        for (long[][] row : data) {
            bh.consume(Intersections.intersection4(row[0],row[1]));
        }
    }

    @Setup
    public void setup() throws IOException {
    }

    @TearDown
    public void shutdown() {
    }
}
