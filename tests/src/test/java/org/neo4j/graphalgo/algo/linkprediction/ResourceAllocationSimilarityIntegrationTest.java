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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.linkprediction.LinkPredictionFunc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResourceAllocationSimilarityIntegrationTest {
    private static final String DB_CYPHER =
            "CREATE (mark:Person {name: 'Mark'})\n" +
            "CREATE (michael:Person {name: 'Michael'})\n" +
            "CREATE (praveena:Person {name: 'Praveena'})\n" +
            "CREATE (ryan:Person {name: 'Ryan'})\n" +
            "CREATE (karin:Person {name: 'Karin'})\n" +
            "CREATE (jennifer:Person {name: 'Jennifer'})\n" +
            "CREATE (elaine:Person {name: 'Elaine'})\n" +

            "MERGE (jennifer)-[:FRIENDS]-(ryan)\n" +
            "MERGE (jennifer)-[:FRIENDS]-(karin)\n" +
            "MERGE (elaine)-[:FRIENDS]-(ryan)\n" +
            "MERGE (elaine)-[:FRIENDS]-(karin)\n" +

            "MERGE (mark)-[:FRIENDS]-(michael)\n" +
            "MERGE (mark)-[:WORKS_WITH]->(michael)\n" +

            "MERGE (praveena)-[:FRIENDS]->(michael)";

    private static GraphDatabaseService db;

    @BeforeAll
    static void setUp() throws KernelException {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.procedure_unrestricted,"algo.*")
                .newGraphDatabase();

        ((GraphDatabaseAPI) db).getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerFunction(LinkPredictionFunc.class);

        db.execute(DB_CYPHER).close();
    }

    @AfterAll
    static void tearDown() {
        db.shutdown();
    }

    @Test
    void oneNodeInCommon() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Mark'})\n" +
                "MATCH (p2:Person {name: 'Praveena'})\n" +
                "RETURN algo.linkprediction.resourceAllocation(p1, p2) AS score, " +
                "       1/3.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void oneNodeInCommonExplicit() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Mark'})\n" +
                        "MATCH (p2:Person {name: 'Praveena'})\n" +
                        "RETURN algo.linkprediction.resourceAllocation(p1, p2, " +
                        "{relationshipQuery: 'FRIENDS', direction: 'BOTH'}) AS score," +
                        "1/2.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void twoNodesInCommon() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Elaine'})\n" +
                        "RETURN algo.linkprediction.resourceAllocation(p1, p2) AS score, " +
                        "       1/2.0 + 1/2.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void noNeighbors() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Ryan'})\n" +
                        "RETURN algo.linkprediction.resourceAllocation(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void bothNodesTheSame() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Praveena'})\n" +
                        "MATCH (p2:Person {name: 'Praveena'})\n" +
                        "RETURN algo.linkprediction.resourceAllocation(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

}
