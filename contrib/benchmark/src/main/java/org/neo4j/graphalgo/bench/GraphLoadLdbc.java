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
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class GraphLoadLdbc {

    @Param({"HEAVY", "HUGE"})
    GraphImpl graph;

    @Param({"true", "false"})
    boolean parallel;

    @Param({"IN", "IN_SORT", "OUT", "OUT_SORT", "BOTH", "BOTH_SORT", "UNDIRECTED", "NONE"})
    DirectionParam dir;

    @Param({"", "length"})
    String weightProp;

    @Param({"", "Person"})
    String label;

    @Param({"L01", "L10"})
    String graphId;

    private GraphDatabaseAPI db;
    private GraphLoader loader;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb(graphId);
        loader = dir.apply(new GraphLoader(db)
                .withOptionalRelationshipWeightsFromProperty(weightProp.isEmpty() ? null : weightProp, 1.0)
                .withOptionalLabel(label.isEmpty() ? null : label));
        if (parallel) {
            loader.withExecutorService(Pools.DEFAULT);
        }
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public void load(Blackhole bh) {
        Graph graph = loader.load(this.graph.impl);
        bh.consume(graph);
        graph.release();
        bh.consume(loader);
    }
}
