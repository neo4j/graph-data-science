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

import org.neo4j.graphalgo.JaccardProc;
import org.neo4j.graphalgo.NeighborhoodSimilarityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.wcc.WccProc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(1)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class JaccardBenchmark {

    private GraphDatabaseAPI db;

    @Setup
    public void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(NeighborhoodSimilarityProc.class);
        procedures.registerProcedure(JaccardProc.class);
        procedures.registerProcedure(WccProc.class);
        createGraph(db);
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Object _01_benchmark() {
        String query = "CALL algo.jaccard.stream('', 'LIKES', {})" +
            " YIELD node1, node2, similarity" +
            " RETURN COUNT(*) AS count";
        return db.execute(query).stream().count();
    }

    @Benchmark
    public Object _02_benchmark() {
        String query = " MATCH (person:Person)-[:LIKES]->(item:Item)" +
                       " WITH {item: id(person), categories: collect(id(item))} AS userData " +
                       " WITH collect(userData) AS data" +
                       " CALL algo.similarity.jaccard.stream(data, {concurrency: 1})" +
                       " YIELD item1, item2, similarity" +
                       " RETURN COUNT(*) AS count";
        return db.execute(query).stream().count();
    }

    @Benchmark
    public Object _03_benchmark() {
        String query = " MATCH (person:Person)-[:LIKES]->(item:Item)" +
                       " WITH {item: id(person), categories: collect(id(item))} AS userData " +
                       " WITH collect(userData) AS data" +
                       " CALL algo.similarity.jaccard.stream(data, {concurrency: 1, topk: 1})" +
                       " YIELD item1, item2, similarity" +
                       " RETURN COUNT(*) AS count";
        return db.execute(query).stream().count();
    }

    @Benchmark
    public Object _04_benchmark() {
        String query = " CALL algo.wcc('', '', {concurrency: 1, write: true, writeProperty: 'setId'})" +
                       " YIELD setCount";
//                       " RETURN COUNT(*) AS count";
        return db.execute(query).stream().count();
    }



    static void createGraph(GraphDatabaseService db) {
        int itemCount = 1_000;
        Label itemLabel = Label.label("Item");
        int personCount = 10_000;
        Label personLabel = Label.label("Person");
        RelationshipType likesType = RelationshipType.withName("LIKES");

        List<Node> itemNodes = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < itemCount; i++) {
                itemNodes.add(db.createNode(itemLabel));
            }
            for (int i = 0; i < personCount; i++) {
                Node person = db.createNode(personLabel);
                if (i % 6 == 0) {
                    int itemIndex = Math.floorDiv(i, 15);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    if (itemIndex > 0) person.createRelationshipTo(itemNodes.get(itemIndex - 1), likesType);
                }
                if (i % 5 == 0) {
                    int itemIndex = Math.floorDiv(i, 10);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    if (itemIndex + 1 < itemCount) person.createRelationshipTo(itemNodes.get(itemIndex + 1), likesType);
                }
                if (i % 4 == 0) {
                    int itemIndex = Math.floorDiv(i, 20);
                    person.createRelationshipTo(itemNodes.get(itemIndex), likesType);
                    person.createRelationshipTo(itemNodes.get(itemIndex + 10), likesType);
                }
            }
            tx.success();
        }
    }

}
