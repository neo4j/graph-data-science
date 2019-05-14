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
package org.neo4j.graphalgo.impl.msbfs;

import org.neo4j.graphalgo.bench.RunSafely;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4G", "-Xmx4G"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MSBFSBenchmark {

    @Param({
//            "_1024_64",
//            "_1024_128",
            "_1024_1024",

//            "_4096_64",
//            "_4096_128",
//            "_4096_1024",
            "_4096_4096",

//            "_16384_64",
//            "_16384_128",
//            "_16384_1024",
//            "_16384_4096",
            "_16384_16384",
    })
    public MSBFSSource source;

    @TearDown
    public void shutdown() {
        Pools.DEFAULT.shutdown();
    }

    @Benchmark
    public MultiSourceBFS measure(Blackhole bh) throws Throwable {
        MultiSourceBFS msbfs = new MultiSourceBFS(
                source.nodes,
                source.rels,
                Direction.OUTGOING,
                consume(bh),
                AllocationTracker.EMPTY,
                source.hugeSources);
        RunSafely.runSafe(() -> msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT));
        return msbfs;
    }

    private static BfsConsumer consume(Blackhole bh) {
        return (i, d, s) -> bh.consume(i);
    }
}
