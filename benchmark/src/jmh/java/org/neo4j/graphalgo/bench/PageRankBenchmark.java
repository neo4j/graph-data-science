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

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
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
 * @author mknobloch
 */
@Threads(1)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PageRankBenchmark {

    @Param({"5", "20", "100"})
    int iterations;

    @Param({"VIEW", "HUGE"})
    GraphImpl impl;

    private GraphDatabaseAPI db;

    @Setup
    public void setup() {
        String createGraph = "CREATE (nA)\n" +
                "CREATE (nB)\n" +
                "CREATE (nC)\n" +
                "CREATE (nD)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" +
                "CREATE (nK)\n" +
                "CREATE\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nB),\n" +
                "  (nD)-[:TYPE]->(nA),\n" +
                "  (nD)-[:TYPE]->(nB),\n" +
                "  (nE)-[:TYPE]->(nB),\n" +
                "  (nE)-[:TYPE]->(nD),\n" +
                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nB),\n" +
                "  (nF)-[:TYPE]->(nE),\n" +
                "  (nG)-[:TYPE]->(nB),\n" +
                "  (nG)-[:TYPE]->(nE),\n" +
                "  (nH)-[:TYPE]->(nB),\n" +
                "  (nH)-[:TYPE]->(nE),\n" +
                "  (nI)-[:TYPE]->(nB),\n" +
                "  (nI)-[:TYPE]->(nE),\n" +
                "  (nJ)-[:TYPE]->(nE),\n" +
                "  (nK)-[:TYPE]->(nE);";
        db = (GraphDatabaseAPI)
                new TestGraphDatabaseFactory()
                        .newImpermanentDatabaseBuilder()
                        .newGraphDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
    }

    /*
    @Benchmark
    public PageRankResult run() throws Exception {
        final Graph graph = new GraphLoader(db)
                .withDirection(Direction.OUTGOING)
                .load(impl.impl);
        try {
            return PageRankAlgorithm
                    .of(graph, 0.85, LongStream.empty())
                    .compute(iterations)
                    .result();
        } finally {
            graph.release();
        }
    }
    */
}
