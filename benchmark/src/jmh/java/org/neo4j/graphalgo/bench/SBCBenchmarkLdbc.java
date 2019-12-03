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

import org.neo4j.graphalgo.BetweennessCentralityProc;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphdb.Transaction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g", "-XX:+UseG1GC"})
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class SBCBenchmarkLdbc extends BaseBenchmark {

    private Transaction tx;

    @Param({"8"})
    int threads;

    @Setup
    public void setup() throws Exception {
        db = LdbcDownloader.openDb();
        registerProcedures(BetweennessCentralityProc.class);
    }

    @Setup(Level.Invocation)
    public void startTx() {
        tx = db.beginTx();
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @TearDown(Level.Invocation)
    public void failTx() {
        tx.failure();
        tx.close();
    }

    @Benchmark
    public void _01_sbcParallel() {
        runQuery(
            "CALL algo.betweenness.sampled(null, null, {strategy:'random', probability:0.001, maxDepth:5, concurrency:" + threads + "}) "
            + "YIELD loadMillis, computeMillis, writeMillis",
            row -> {
                long load = row.getNumber("loadMillis").longValue();
                long compute = row.getNumber("computeMillis").longValue();
                long write = row.getNumber("writeMillis").longValue();

                System.out.println("load = " + load);
                System.out.println("compute = " + compute);
                System.out.println("write = " + write);
            }
        );
    }
}
