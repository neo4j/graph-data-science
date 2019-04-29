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
package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.EigenvectorCentralityProc;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class EigenvectorCentralityProcNormalizationIntegrationTest {

    private static GraphDatabaseAPI db;
    private static Map<Long, Double> expected = new HashMap<>();
    private static Map<Long, Double> maxNormExpected = new HashMap<>();
    private static Map<Long, Double> l2NormExpected = new HashMap<>();
    private static Map<Long, Double> l1NormExpected = new HashMap<>();

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @BeforeClass
    public static void setup() throws KernelException {
        ClassLoader classLoader = EigenvectorCentralityProcNormalizationIntegrationTest.class.getClassLoader();
        File file = new File(classLoader.getResource("got/got-s1-nodes.csv").getFile());

        db = (GraphDatabaseAPI)new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()))
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root,file.getParent())
                .setConfig(GraphDatabaseSettings.procedure_unrestricted,"algo.*")
                .newGraphDatabase();


        try (Transaction tx = db.beginTx()) {
            db.execute("CREATE CONSTRAINT ON (c:Character)\n" +
                    "ASSERT c.id IS UNIQUE;").close();
        }

        try (Transaction tx = db.beginTx()) {
            db.execute("LOAD CSV WITH HEADERS FROM 'file:///got-s1-nodes.csv' AS row\n" +
                    "MERGE (c:Character {id: row.Id})\n" +
                    "SET c.name = row.Label;").close();

            db.execute("LOAD CSV WITH HEADERS FROM 'file:///got-s1-edges.csv' AS row\n" +
                    "MATCH (source:Character {id: row.Source})\n" +
                    "MATCH (target:Character {id: row.Target})\n" +
                    "MERGE (source)-[rel:INTERACTS_SEASON1]->(target)\n" +
                    "SET rel.weight = toInteger(row.Weight);").close();

            tx.success();
        }

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(EigenvectorCentralityProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerFunction(GetNodeFunc.class);



        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Character");
            expected.put(db.findNode(label, "name", "Ned").getId(), 111.68570401574802);
            expected.put(db.findNode(label, "name", "Robert").getId(), 88.09448401574804);
            expected.put(db.findNode(label, "name", "Cersei").getId(), 		84.59226401574804);
            expected.put(db.findNode(label, "name", "Catelyn").getId(), 	84.51566401574803);
            expected.put(db.findNode(label, "name", "Tyrion").getId(), 82.00291401574802);
            expected.put(db.findNode(label, "name", "Joffrey").getId(), 77.67397401574803);
            expected.put(db.findNode(label, "name", "Robb").getId(), 73.56551401574802);
            expected.put(db.findNode(label, "name", "Arya").getId(), 73.32532401574804	);
            expected.put(db.findNode(label, "name", "Petyr").getId(), 72.26733401574802);
            expected.put(db.findNode(label, "name", "Sansa").getId(), 71.56470401574803);
        }

        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Character");
            maxNormExpected.put(db.findNode(label, "name", "Ned").getId(), 1.0);
            maxNormExpected.put(db.findNode(label, "name", "Robert").getId(), 0.78823475553106);
            maxNormExpected.put(db.findNode(label, "name", "Cersei").getId(), 		0.7567972769062152);
            maxNormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 	0.7561096813631987);
            maxNormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 0.7335541239126161);
            maxNormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 0.694695640231341);
            maxNormExpected.put(db.findNode(label, "name", "Robb").getId(), 0.6578162827292336);
            maxNormExpected.put(db.findNode(label, "name", "Arya").getId(), 0.6556602308561643);
            maxNormExpected.put(db.findNode(label, "name", "Petyr").getId(), 0.6461632437992975);
            maxNormExpected.put(db.findNode(label, "name", "Sansa").getId(), 0.6398561255696676);
        }

        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Character");
            l2NormExpected.put(db.findNode(label, "name", "Ned").getId(), 0.31424020248680057);
            l2NormExpected.put(db.findNode(label, "name", "Robert").getId(), 0.2478636701002979);
            l2NormExpected.put(db.findNode(label, "name", "Cersei").getId(), 		0.23800978296539527);
            l2NormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 	0.23779426030989856);
            l2NormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 0.23072435753445136);
            l2NormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 0.21854440134273145);
            l2NormExpected.put(db.findNode(label, "name", "Robb").getId(), 0.2069847902565455);
            l2NormExpected.put(db.findNode(label, "name", "Arya").getId(), 0.20630898886459065);
            l2NormExpected.put(db.findNode(label, "name", "Petyr").getId(), 0.20333221583209818);
            l2NormExpected.put(db.findNode(label, "name", "Sansa").getId(), 0.20135528784996212);
        }

        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Character");
            l1NormExpected.put(db.findNode(label, "name", "Ned").getId(), 0.04193172127455592);
            l1NormExpected.put(db.findNode(label, "name", "Robert").getId(), 0.03307454057909963);
            l1NormExpected.put(db.findNode(label, "name", "Cersei").getId(), 		0.031759653287334266);
            l1NormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 	0.03173089428117553);
            l1NormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 0.030787497509304138);
            l1NormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 0.029162223199633484);
            l1NormExpected.put(db.findNode(label, "name", "Robb").getId(), 0.027619726770875055);
            l1NormExpected.put(db.findNode(label, "name", "Arya").getId(), 0.027529548889814154);
            l1NormExpected.put(db.findNode(label, "name", "Petyr").getId(), 0.027132332950834063);
            l1NormExpected.put(db.findNode(label, "name", "Sansa").getId(), 0.026868534772018032);
        }
    }

    @Test
    public void noNormalizing() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector.stream('Character', 'INTERACTS_SEASON1', {direction: 'BOTH'}) " +
                        "YIELD nodeId, score " +
                        "RETURN nodeId, score, algo.asNode(nodeId).name AS name " +
                        "ORDER BY score DESC " +
                        "LIMIT 10",
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(expected, actual);
    }

    @Test
    public void maxNorm() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector.stream('Character', 'INTERACTS_SEASON1', {direction: 'BOTH', normalization: 'max'}) " +
                        "YIELD nodeId, score " +
                        "RETURN nodeId, score " +
                        "ORDER BY score DESC " +
                        "LIMIT 10",
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(maxNormExpected, actual);
    }

    @Test
    public void l2Norm() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector.stream('Character', 'INTERACTS_SEASON1', {direction: 'BOTH', normalization: 'l2Norm'}) " +
                        "YIELD nodeId, score " +

                        "RETURN nodeId, score, algo.asNode(nodeId).name AS name " +
                        "ORDER BY score DESC " +
                        "LIMIT 10",
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(l2NormExpected, actual);
    }

    @Test
    public void l1Norm() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector.stream('Character', 'INTERACTS_SEASON1', {direction: 'BOTH', normalization: 'l1Norm'}) " +
                        "YIELD nodeId, score " +
                        "RETURN nodeId, score " +
                        "ORDER BY score DESC " +
                        "LIMIT 10",
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(l1NormExpected, actual);
    }

    @Test
    public void l1NormWrite() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector('Character', 'INTERACTS_SEASON1', {direction: 'BOTH', normalization: 'l1Norm', writeProperty: 'eigen'}) ",
                row -> {});

        runQuery(
                "MATCH (c:Character) " +
                       "RETURN id(c) AS nodeId, c.eigen AS score " +
                       "ORDER BY score DESC " +
                       "LIMIT 10",
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(l1NormExpected, actual);
    }

    private static void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        runQuery(query, new HashMap<>(), check);
    }

    private static void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }

    private void assertResult(final String scoreProperty, Map<Long, Double> expected) {
        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) db
                        .getNodeById(entry.getKey())
                        .getProperty(scoreProperty)).doubleValue();
                assertEquals(
                        "score for " + entry.getKey(),
                        entry.getValue(),
                        score,
                        0.1);
            }
            tx.success();
        }
    }

    private static void assertMapEquals(
            Map<Long, Double> expected,
            Map<Long, Double> actual) {
        assertEquals("number of elements", expected.size(), actual.size());
        HashSet<Long> expectedKeys = new HashSet<>(expected.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                    "unknown key " + entry.getKey(),
                    expectedKeys.remove(entry.getKey()));
            assertEquals(
                    "value for " + entry.getKey(),
                    expected.get(entry.getKey()),
                    entry.getValue(),
                    0.1);
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }
}
