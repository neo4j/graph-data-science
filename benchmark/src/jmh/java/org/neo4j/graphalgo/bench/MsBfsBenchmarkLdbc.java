/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Threads(1)
@Fork(value = 3, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MsBfsBenchmarkLdbc {

    @Param({"L01", "L10"})
    String graphId;

    private GraphDatabaseAPI db;
    private Graph grph;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb(graphId);
        grph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .build()
            .graph(HugeGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        grph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public MultiSourceBFS run(Blackhole bh) throws Throwable {
        LongAdder tpt = new LongAdder();
        long start = System.nanoTime();
        AtomicLong lastLog = new AtomicLong(start);
        long interval = TimeUnit.SECONDS.toNanos(1L);
        Log log = FormattedLog.withLogLevel(Level.DEBUG).toOutputStream(System.out);
        MultiSourceBFS msbfs = new MultiSourceBFS(
                grph,
                grph,
                (i, d, s) -> {
                    tpt.add(s.size());
                    long ll = lastLog.get();
                    long current = System.nanoTime();
                    if (current > ll + interval && lastLog.compareAndSet(ll, current)) {
                        long edges = tpt.longValue();
                        long seconds = TimeUnit.NANOSECONDS.toSeconds(current - start);
                        long megaEdgesTraversedPerSecond = (edges / seconds) >> 20;
                        log.info("[%s] thrpt in METPS %d", Thread.currentThread().getName(), megaEdgesTraversedPerSecond);
                    }
                }, AllocationTracker.EMPTY
        );
        RunSafely.runSafe(() -> msbfs.run(Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT));
        return msbfs;
    }
}
