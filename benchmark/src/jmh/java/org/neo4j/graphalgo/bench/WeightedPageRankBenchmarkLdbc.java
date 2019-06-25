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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.impl.pagerank.PageRankFactory;
import org.neo4j.graphalgo.impl.results.CentralityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
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
import java.util.stream.LongStream;

@Threads(1)
@Fork(value = 3, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 5, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class WeightedPageRankBenchmarkLdbc {

    //    @Param({"HEAVY", "HUGE"})
    @Param({"HEAVY"})
    GraphImpl graph;

    //        @Param({"true", "false"})
    @Param({"false"})
    boolean parallel;

    //    @Param({"L01", "L10"})
    @Param({"L01"})
    String graphId;
    ;

    @Param({"5", "20"})
//    @Param({"5"})
            int iterations;

    @Param({"true", "false"})
    boolean cacheWeights;

    private GraphDatabaseAPI db;
    private Graph grph;
    private int batchSize;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb(graphId);

        Transaction tx = db.beginTx();
        int count = 0;
        for (Relationship relationship : db.getAllRelationships()) {
            long startNodeId = relationship.getStartNodeId();
            long endNodeId = relationship.getEndNodeId();
            relationship.setProperty("weight", startNodeId + endNodeId % 100);
            if (++count % 100000 == 0) {
                tx.success();
                tx.close();
                tx = db.beginTx();
            }
        }

        tx.success();
        tx.close();

        grph = new GraphLoader(db, Pools.DEFAULT)
                .withDirection(Direction.OUTGOING)
                .withRelationshipWeightsFromProperty("weight", 1.0)
                .load(graph.impl);

        batchSize = parallel ? 10_000 : 2_000_000_000;
    }

    @TearDown
    public void shutdown() {
        grph.release();
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public CentralityResult run() throws Exception {
        return PageRankFactory
                .weightedOf(
                        grph,
                        0.85,
                        LongStream.empty(),
                        AllocationTracker.EMPTY,
                        Pools.DEFAULT,
                        Pools.DEFAULT_CONCURRENCY,
                        batchSize,
                        cacheWeights)
                .compute(iterations)
                .result();
    }
}
