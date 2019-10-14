/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.loading.NullPropertyMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.wcc.WCC;
import org.neo4j.graphalgo.impl.wcc.WCCType;
import org.neo4j.graphdb.Direction;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms16g", "-Xmx16g", "-XX:+UseG1GC"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class WeaklyConnectedComponentsLdbc {

    @Param({"L01"})
    String graphId;

    private GraphDatabaseAPI db;
    private Graph graph;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb(graphId);
        graph = new GraphLoader(db, Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .load(HugeGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        graph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public DisjointSetStruct parallel() {
        return WCCType.PARALLEL
                .create(
                        graph,
                        Pools.DEFAULT,
                        ParallelUtil.DEFAULT_BATCH_SIZE,
                        Pools.DEFAULT_CONCURRENCY,
                        new WCC.Config(new NullPropertyMap(1.0), 0),
                        AllocationTracker.EMPTY)
                .compute();
    }

    @Benchmark
    public DisjointSetStruct pregel() {
        return WCCType.PREGEL
                .create(
                        graph,
                        Pools.DEFAULT,
                        ParallelUtil.DEFAULT_BATCH_SIZE,
                        Pools.DEFAULT_CONCURRENCY,
                        new WCC.Config(new NullPropertyMap(1.0), 0),
                        AllocationTracker.EMPTY)
                .compute();

    }
}
