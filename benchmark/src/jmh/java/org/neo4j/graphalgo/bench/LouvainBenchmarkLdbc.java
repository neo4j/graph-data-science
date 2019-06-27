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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.louvain.Louvain;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
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
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms16g", "-Xmx16g", "-XX:+UseG1GC"})
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 4, time = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class LouvainBenchmarkLdbc {

    @Param({"1"})
    int iterations;

    @Param({"1"})
    int threads;

    @Param({"L01"})
    String db;

    @Param({"HUGE"})
    GraphImpl graph;

    private Graph g;
    private GraphDatabaseAPI gdb;

    @Setup
    public void setup() throws IOException {
        gdb = LdbcDownloader.openDb(db);
        g = new GraphLoader(gdb)
                .withConcurrency(Pools.DEFAULT_CONCURRENCY)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withOptionalRelationshipWeightsFromProperty(null, 1.0)
                .asUndirected(true)
                .load(graph.impl);
    }

    @TearDown
    public void shutdown() {
        gdb.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Louvain louvain() {
        return new Louvain(g, new Louvain.Config(iterations), Pools.DEFAULT, threads, AllocationTracker.EMPTY)
                .withTerminationFlag(TerminationFlag.RUNNING_TRUE)
                .withProgressLogger(ProgressLogger.NULL_LOGGER)
                .compute(iterations, iterations);
    }
}
