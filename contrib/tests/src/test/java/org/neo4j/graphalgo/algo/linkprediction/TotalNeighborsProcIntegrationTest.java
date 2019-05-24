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
package org.neo4j.graphalgo.algo.linkprediction;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.linkprediction.LinkPredictionFunc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TotalNeighborsProcIntegrationTest {
    private static final String SETUP =
            "CREATE (mark:Person {name: 'Mark'})\n" +
            "CREATE (michael:Person {name: 'Michael'})\n" +
            "CREATE (praveena:Person {name: 'Praveena'})\n" +
            "CREATE (jennifer:Person {name: 'Jennifer'})\n" +

            "MERGE (jennifer)-[:FRIENDS]-(mark)\n" +
            "MERGE (mark)-[:FRIENDS]-(praveena)\n" +
            "MERGE (jennifer)-[:FOLLOWS]->(praveena)\n" +
            "MERGE (praveena)-[:FOLLOWS]->(michael)";

    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.procedure_unrestricted,"algo.*")
                .newGraphDatabase();

        ((GraphDatabaseAPI) db).getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerFunction(LinkPredictionFunc.class);

        db.execute(SETUP).close();
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void sameNodesHaveNeighborCount() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Jennifer'})\n" +
                        "RETURN algo.linkprediction.totalNeighbors(p1, p2) AS score, " +
                        "       2.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    public void countDuplicateNeighboursOnce() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN algo.linkprediction.totalNeighbors(p1, p2) AS score, " +
                        "       3.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    public void theOtherNodeCountsAsANeighbor() throws Exception {
        String controlQuery =
                "MATCH (p1:Person {name: 'Praveena'})\n" +
                        "MATCH (p2:Person {name: 'Michael'})\n" +
                        "RETURN algo.linkprediction.totalNeighbors(p1, p2) AS score, " +
                        "       4.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }



}
