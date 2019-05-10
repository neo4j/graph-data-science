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

import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx4g", "-XX:+UseG1GC"})
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class LouvainBenchmarkLdbc {

    private GraphDatabaseAPI db;
    private Transaction tx;

    @Param({"HEAVY", "HUGE"})
    GraphImpl graph;

    @Param({"1", "4"})
    int threads;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LouvainProc.class);
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
    public Object _01_louvainParallel() {
        return runQuery(
                db,
                "CALL algo.louvain(null, null, {maxIterations:1, concurrency: $threads, graph: $graph}) "
                        + "YIELD loadMillis, computeMillis, writeMillis, nodes, communityCount, iterations"
                , r -> {
                    long load = r.getNumber("loadMillis").longValue();
                    long compute = r.getNumber("computeMillis").longValue();
                    long write = r.getNumber("writeMillis").longValue();
                    long count = r.getNumber("communityCount").longValue();
                    long iter = r.getNumber("iterations").longValue();
                    long nodes = r.getNumber("nodes").longValue();

                    System.out.println("communities = " + count);
                    System.out.println("iter = " + iter);
                    System.out.println("nodes = " + nodes);
                    System.out.println("load = " + load);
                    System.out.println("compute = " + compute);
                    System.out.println("write = " + write);

                }
        );
    }

    private Object runQuery(
            GraphDatabaseAPI db,
            String query,
            Consumer<Result.ResultRow> action) {
        try (Result result = db.execute(query, MapUtil.map("threads", threads, "graph", graph.name()))) {
            result.accept(r -> {
                action.accept(r);
                return true;
            });
        }
        return db;
    }
}
