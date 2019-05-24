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

import org.neo4j.graphalgo.InfoMapProc;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author mknobloch
 */
@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms4g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.SECONDS)
@Timeout(time = 2, timeUnit = TimeUnit.HOURS)
public class InfoMapBenchmarkLdbc {

    private GraphDatabaseAPI db;
    private Transaction tx;

    @Setup
    public void setup() throws KernelException, IOException {
        db = LdbcDownloader.openDb("Yelp");

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(InfoMapProc.class);
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
    public Object _01_infoMap() {
        return runQuery(db,
                "CALL algo.infoMap('MATCH (c:Category) RETURN id(c) AS id',\n" +
                        "  'MATCH (c1:Category)<-[:IN_CATEGORY]-()-[:IN_CATEGORY]->(c2:Category)\n" +
                        "   WHERE id(c1) < id(c2)\n" +
                        "   RETURN id(c1) AS source, id(c2) AS target, count(*) AS w', " +
                        " {graph: 'cypher', iterations:15, writeProperty:'c', threshold:0.01, tau:0.3, weightProperty:'w', concurrency:4})",
                r -> System.out.printf("communities: %d | iterations %d | nodes %d | load %d ms | compute %d ms | write %d ms | ",
                        r.getNumber("communityCount").longValue(),
                        r.getNumber("iterations").longValue(),
                        r.getNumber("nodeCount").longValue(),
                        r.getNumber("loadMillis").longValue(),
                        r.getNumber("computeMillis").longValue(),
                        r.getNumber("writeMillis").longValue())
        );
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
