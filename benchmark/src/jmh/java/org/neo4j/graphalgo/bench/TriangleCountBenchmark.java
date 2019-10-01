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
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.impl.triangle.TriangleCountForkJoin;
import org.neo4j.graphalgo.impl.triangle.TriangleStream;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
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

import java.util.concurrent.TimeUnit;

/**
 * @author mknblch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class TriangleCountBenchmark {

    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";
    private static final int TRIANGLE_COUNT = 500;

    private Graph g;
    private GraphDatabaseAPI api;

    @Param({"0.2", "0.5", "0.8"})
    private double connectedness;

    @Param({"true", "false"})
    private boolean parallel;

    private int concurrency;
    private int seqThreshold;

    @Setup
    public void setup() {
        api = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms for " + TRIANGLE_COUNT + " nodes"))) {
            GraphBuilder.create(api)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newCompleteGraphBuilder()
                    .createCompleteGraph(TRIANGLE_COUNT, connectedness);
        }

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            g = new GraphLoader(api)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .sorted()
                    .undirected()
                    .load(HugeGraphFactory.class);
        }

        concurrency = parallel ? Pools.DEFAULT_CONCURRENCY : 1;
        seqThreshold = parallel ? ParallelUtil.threadCount(Pools.DEFAULT_CONCURRENCY, TRIANGLE_COUNT) : 2 * TRIANGLE_COUNT;
    }


    @TearDown
    public void tearDown() {
        if (api != null) api.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Object _02_forkJoin() {
        return new TriangleCountForkJoin(g, Pools.FJ_POOL, seqThreshold).compute().getTriangleCount();
    }

    @Benchmark
    public Object _03_stream() {
        return new TriangleStream(g, Pools.DEFAULT, concurrency).resultStream().count();
    }
}
