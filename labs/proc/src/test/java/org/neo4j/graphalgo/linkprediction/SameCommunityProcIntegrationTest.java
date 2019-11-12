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
package org.neo4j.graphalgo.linkprediction;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

class SameCommunityProcIntegrationTest {

    private static final String DB_CYPHER =
            "CREATE (mark:Person {name: 'Mark'})\n" +
            "SET mark.community=1, mark.partition = 4\n" +
            "CREATE (michael:Person {name: 'Michael'})\n" +
            "SET michael.community=2, michael.partition=4\n" +
            "CREATE (praveena:Person {name: 'Praveena'})\n" +
            "SET praveena.community=1\n" +
            "CREATE (jennifer:Person {name: 'Jennifer'})\n";

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
    void missingProperty() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Jennifer'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN algo.linkprediction.sameCommunity(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void sameCommunity() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Praveena'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN algo.linkprediction.sameCommunity(p1, p2) AS score, " +
                        "       1.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void differentCommunity() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Michael'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN algo.linkprediction.sameCommunity(p1, p2) AS score, " +
                        "       0.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }

    @Test
    void specifyProperty() {
        String controlQuery =
                "MATCH (p1:Person {name: 'Michael'})\n" +
                        "MATCH (p2:Person {name: 'Mark'})\n" +
                        "RETURN algo.linkprediction.sameCommunity(p1, p2, 'partition') AS score, " +
                        "       1.0 AS cypherScore";

        try (Transaction tx = db.beginTx()) {
            Result result = db.execute(controlQuery);
            Map<String, Object> node = result.next();
            assertEquals((Double) node.get("cypherScore"), (double) node.get("score"), 0.01);
        }
    }



}
