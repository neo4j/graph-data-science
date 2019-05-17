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

import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.proc.LabelPropagationProc;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 1, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class LabelPropagationBenchmarkLdbc {

    @Param({"1", "5"})
    int iterations;

    @Param({"10000", "100000000"})
    int batchSize;

    private HeavyGraph graph;
    private GraphDatabaseAPI db;
    private Transaction tx;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb();

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LabelPropagationProc.class);

        graph = (HeavyGraph) new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipWeightsFromProperty("weight", 1.0d)
                .withOptionalNodeWeightsFromProperty("weight", 1.0d)
                .withOptionalNodeProperty("partition", 0.0d)
                .withExecutorService(Pools.DEFAULT)
                .load(HeavyGraphFactory.class);
    }

    @Setup(Level.Invocation)
    public void startTx() {
        tx = db.beginTx();
    }

    @TearDown
    public void shutdown() {
        graph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @TearDown(Level.Invocation)
    public void failTx() {
        tx.failure();
        tx.close();
    }

    @Benchmark
    public Object _01_algo() {
        return runPrintQuery(
                db,
                "CALL algo.labelPropagation(null, null, 'OUTGOING',{iterations:" + iterations + ",batchSize:" + batchSize + "}) "
                        + "YIELD loadMillis, computeMillis, writeMillis"
        );
    }

    @Benchmark
    public Object _03_direct() {
        return new LabelPropagation(graph, graph, batchSize, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT, AllocationTracker.EMPTY)
                .compute(Direction.OUTGOING, iterations);
    }

    private static Object runPrintQuery(GraphDatabaseAPI db, String query) {
        return runQuery(db, query, result -> {
            long load = result.getNumber("loadMillis").longValue();
            long compute = result.getNumber("computeMillis").longValue();
            long write = result.getNumber("writeMillis").longValue();
            System.out.printf(
                    "load: %d ms%ncompute: %d ms%nwrite: %d ms%n",
                    load,
                    compute,
                    write);
        });
    }

    private static Object runVoidQuery(GraphDatabaseAPI db, String query) {
        return runQuery(db, query, r -> {});
    }

    private static Object runQuery(
            GraphDatabaseAPI db,
            String query,
            Consumer<Result.ResultRow> action) {
        try (Result result = db.execute(query)) {
            result.accept(r -> {
                action.accept(r);
                return true;
            });
        }
        return db;
    }
}
