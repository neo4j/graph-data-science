/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.centrality.eigenvector;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphdb.Label;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

class EigenvectorCentralityProcTest extends BaseProcTest {

    private static final Map<Long, Double> noNormExpected = new HashMap<>();
    private static final Map<Long, Double> maxNormExpected = new HashMap<>();
    private static final Map<Long, Double> l2NormExpected = new HashMap<>();
    private static final Map<Long, Double> l1NormExpected = new HashMap<>();
    public static final String EXPLICIT_GRAPH_NAME = "eigenvectorTest";

    @AfterEach
    void tearDown() {
        GraphCatalog.removeAllLoadedGraphs();
        db.shutdown();
    }

    @BeforeEach
    void setup() throws Exception {
        ClassLoader classLoader = EigenvectorCentralityProcTest.class.getClassLoader();
        File file = new File(classLoader.getResource("got/got-s1-nodes.csv").getFile());
        db = TestDatabaseCreator.createTestDatabaseWithCustomLoadCsvRoot(file.getParent());

        runQuery("CREATE CONSTRAINT ON (c:Character)\n" +
                 "ASSERT c.id IS UNIQUE;");

        runQuery("LOAD CSV WITH HEADERS FROM 'file:///got-s1-nodes.csv' AS row\n" +
                 "MERGE (c:Character {id: row.Id})\n" +
                 "SET c.name = row.Label;");

        runQuery("LOAD CSV WITH HEADERS FROM 'file:///got-s1-edges.csv' AS row\n" +
                 "MATCH (source:Character {id: row.Source})\n" +
                 "MATCH (target:Character {id: row.Target})\n" +
                 "MERGE (source)-[rel:INTERACTS_SEASON1]->(target)\n" +
                 "SET rel.weight = toInteger(row.Weight);");

        registerProcedures(
            EigenvectorCentralityProc.class,
            GraphCreateProc.class
        );

        runInTransaction(db, () -> {
            final Label label = Label.label("Character");
            noNormExpected.put(db.findNode(label, "name", "Ned").getId(), 111.68570401574802);
            noNormExpected.put(db.findNode(label, "name", "Robert").getId(), 88.09448401574804);
            noNormExpected.put(db.findNode(label, "name", "Cersei").getId(), 84.59226401574804);
            noNormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 84.51566401574803);
            noNormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 82.00291401574802);
            noNormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 77.67397401574803);
            noNormExpected.put(db.findNode(label, "name", "Robb").getId(), 73.56551401574802);
            noNormExpected.put(db.findNode(label, "name", "Arya").getId(), 73.32532401574804);
            noNormExpected.put(db.findNode(label, "name", "Petyr").getId(), 72.26733401574802);
            noNormExpected.put(db.findNode(label, "name", "Sansa").getId(), 71.56470401574803);
        });

        runInTransaction(db, () -> {
            final Label label = Label.label("Character");
            maxNormExpected.put(db.findNode(label, "name", "Ned").getId(), 1.0);
            maxNormExpected.put(db.findNode(label, "name", "Robert").getId(), 0.78823475553106);
            maxNormExpected.put(db.findNode(label, "name", "Cersei").getId(), 0.7567972769062152);
            maxNormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 0.7561096813631987);
            maxNormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 0.7335541239126161);
            maxNormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 0.694695640231341);
            maxNormExpected.put(db.findNode(label, "name", "Robb").getId(), 0.6578162827292336);
            maxNormExpected.put(db.findNode(label, "name", "Arya").getId(), 0.6556602308561643);
            maxNormExpected.put(db.findNode(label, "name", "Petyr").getId(), 0.6461632437992975);
            maxNormExpected.put(db.findNode(label, "name", "Sansa").getId(), 0.6398561255696676);
        });

        runInTransaction(db, () -> {
            final Label label = Label.label("Character");
            l2NormExpected.put(db.findNode(label, "name", "Ned").getId(), 0.31424020248680057);
            l2NormExpected.put(db.findNode(label, "name", "Robert").getId(), 0.2478636701002979);
            l2NormExpected.put(db.findNode(label, "name", "Cersei").getId(), 0.23800978296539527);
            l2NormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 0.23779426030989856);
            l2NormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 0.23072435753445136);
            l2NormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 0.21854440134273145);
            l2NormExpected.put(db.findNode(label, "name", "Robb").getId(), 0.2069847902565455);
            l2NormExpected.put(db.findNode(label, "name", "Arya").getId(), 0.20630898886459065);
            l2NormExpected.put(db.findNode(label, "name", "Petyr").getId(), 0.20333221583209818);
            l2NormExpected.put(db.findNode(label, "name", "Sansa").getId(), 0.20135528784996212);
        });

        runInTransaction(db, () -> {
            final Label label = Label.label("Character");
            l1NormExpected.put(db.findNode(label, "name", "Ned").getId(), 0.04193172127455592);
            l1NormExpected.put(db.findNode(label, "name", "Robert").getId(), 0.03307454057909963);
            l1NormExpected.put(db.findNode(label, "name", "Cersei").getId(), 0.031759653287334266);
            l1NormExpected.put(db.findNode(label, "name", "Catelyn").getId(), 0.03173089428117553);
            l1NormExpected.put(db.findNode(label, "name", "Tyrion").getId(), 0.030787497509304138);
            l1NormExpected.put(db.findNode(label, "name", "Joffrey").getId(), 0.029162223199633484);
            l1NormExpected.put(db.findNode(label, "name", "Robb").getId(), 0.027619726770875055);
            l1NormExpected.put(db.findNode(label, "name", "Arya").getId(), 0.027529548889814154);
            l1NormExpected.put(db.findNode(label, "name", "Petyr").getId(), 0.027132332950834063);
            l1NormExpected.put(db.findNode(label, "name", "Sansa").getId(), 0.026868534772018032);
        });

        createExplicitGraph(EXPLICIT_GRAPH_NAME);
    }

    @ParameterizedTest(name = "Normalization: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityOnExplicitGraph(String normalizationType, Map<Long, Double> expected) {
        String eigenvectorStreamQuery = GdsCypher.call()
            .explicitCreation(EXPLICIT_GRAPH_NAME)
            .algo("gds", "alpha", "eigenvector")
            .streamMode()
            .addParameter("normalization", normalizationType)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();

        runQueryWithRowConsumer(
            eigenvectorStreamQuery +
            " RETURN nodeId, score" +
            " ORDER BY score DESC" +
            " LIMIT 10",
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "Normalization: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityOnImplicitGraph(String normalizationType, Map<Long, Double> expected) {
        String eigenvectorStreamQuery = GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
            .builder()
            .graphName("eigenvectorImplicitNorm")
            .nodeProjections(NodeProjections.fromString("Character"))
            .relationshipProjections(RelationshipProjections.single(
                ElementIdentifier.of("INTERACTS_SEASON1"),
                RelationshipProjection.builder()
                    .type("INTERACTS_SEASON1")
                    .orientation(Orientation.UNDIRECTED)
                    .build()
            )).build())
            .algo("gds", "alpha", "eigenvector")
            .streamMode()
            .addParameter("normalization", normalizationType)
            .addParameter("writeProperty", "eigen")
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();

        runQueryWithRowConsumer(
            eigenvectorStreamQuery +
            " RETURN nodeId, score" +
            " ORDER BY score DESC" +
            " LIMIT 10",
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );
        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "Normalization: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityWriteOnExplicitGraph(String normalizationType, Map<Long, Double> expected) {

        final Map<Long, Double> actual = new HashMap<>();
        String eigenvectorWriteQuery = GdsCypher.call()
            .explicitCreation(EXPLICIT_GRAPH_NAME)
            .algo("gds", "alpha", "eigenvector")
            .writeMode()
            .addParameter("normalization", normalizationType)
            .addParameter("writeProperty", "eigen")
            .yields();

        runQueryWithRowConsumer(eigenvectorWriteQuery, row -> {
            assertNotEquals(-1L, row.getNumber("loadMillis").longValue());
            assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
            assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
        });

        runQueryWithRowConsumer(
            "MATCH (c:Character) " +
            "RETURN id(c) AS nodeId, c.eigen AS score " +
            "ORDER BY score DESC " +
            "LIMIT 10",
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "Normalization: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityWriteOnImplicitGraph(String normalizationType, Map<Long, Double> expected) {
        final Map<Long, Double> actual = new HashMap<>();
        String eigenvectorWriteQuery = GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
            .builder()
            .graphName("eigenvectorImplicitNorm")
            .nodeProjections(NodeProjections.fromString("Character"))
            .relationshipProjections(RelationshipProjections.single(
                ElementIdentifier.of("INTERACTS_SEASON1"),
                RelationshipProjection.builder()
                    .type("INTERACTS_SEASON1")
                    .orientation(Orientation.UNDIRECTED)
                    .build()
            )).build())
            .algo("gds", "alpha", "eigenvector")
            .writeMode()
            .addParameter("normalization", normalizationType)
            .addParameter("writeProperty", "eigen")
            .yields();

        runQuery(eigenvectorWriteQuery);

        runQueryWithRowConsumer(
            "MATCH (c:Character) " +
            "RETURN id(c) AS nodeId, c.eigen AS score " +
            "ORDER BY score DESC " +
            "LIMIT 10",
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );

        assertMapEquals(expected, actual);
    }

    @ParameterizedTest(name = "Graph Creation: {0}")
    @MethodSource("gdsCyphers")
    void testStreamAllDefaults(String desc, GdsCypher.ModeBuildStage queryBuilder) {
        final Map<Long, Double> actual = new HashMap<>();

        String eigenvectorStreamQuery =
            queryBuilder.streamMode().yields("nodeId", "score") +
            " RETURN nodeId, score" +
            " ORDER BY score DESC" +
            " LIMIT 10";

        runQueryWithRowConsumer(
            eigenvectorStreamQuery,
            row -> actual.put(
                (Long) row.get("nodeId"),
                (Double) row.get("score")
            )
        );
        assertMapEquals(noNormExpected, actual);
    }

    @ParameterizedTest(name = "Graph Creation: {0}")
    @MethodSource("gdsCyphers")
    void testWriteAllDefaults(String desc, GdsCypher.ModeBuildStage queryBuilder) {
        String eigenvectorWriteQuery = queryBuilder.writeMode().yields("writeMillis", "write", "writeProperty");

        runQueryWithRowConsumer(
            eigenvectorWriteQuery,
            row -> {
                assertTrue(row.getBoolean("write"));
                assertEquals("eigenvector", row.getString("writeProperty"));
                assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
            }
        );
        assertResult("eigenvector", noNormExpected);
    }

    @ParameterizedTest(name = "Graph Creation: {0}")
    @MethodSource("gdsCyphers")
    void testParallelWrite(String desc, GdsCypher.ModeBuildStage queryBuilder) {
        String eigenvectorWriteQuery = queryBuilder
            .writeMode()
            .addParameter("concurrency", 2)
            .yields("writeMillis", "writeProperty", "iterations");
        runQueryWithRowConsumer(
            eigenvectorWriteQuery,
            row -> assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set")
        );
        assertResult("eigenvector", noNormExpected);
    }

    @ParameterizedTest(name = "Graph Creation: {0}")
    @MethodSource("gdsCyphers")
    void testParallelStream(String desc, GdsCypher.ModeBuildStage queryBuilder) {
        final Map<Long, Double> actual = new HashMap<>();
        String eigenvectorStreamQuery = queryBuilder
            .streamMode()
            .addParameter("concurrency", 2)
            .yields("nodeId", "score");
        eigenvectorStreamQuery = eigenvectorStreamQuery +
                                 " RETURN nodeId, score " +
                                 "ORDER BY score DESC " +
                                 "LIMIT 10";
        runQueryWithRowConsumer(
            eigenvectorStreamQuery,
            row -> {
                final long nodeId = row.getNumber("nodeId").longValue();
                actual.put(nodeId, (Double) row.get("score"));
            }
        );
        assertMapEquals(noNormExpected, actual);
    }

    static Stream<Arguments> normalizations() {
        return Stream.of(
            arguments("none", noNormExpected),
            arguments("max", maxNormExpected),
            arguments("l1Norm", l1NormExpected),
            arguments("l2Norm", l2NormExpected)
        );
    }

    static Stream<Arguments> gdsCyphers() {
        return Stream.of(
            arguments(
                "explicit",
                GdsCypher.call()
                    .explicitCreation(EXPLICIT_GRAPH_NAME)
                    .algo("gds", "alpha", "eigenvector")
            ),
            arguments(
                "implicit",
                GdsCypher.call().implicitCreation(ImmutableGraphCreateFromStoreConfig
                    .builder()
                    .graphName("eigenvectorImplicitTest")
                    .nodeProjections(NodeProjections.fromString("Character"))
                    .relationshipProjections(RelationshipProjections.single(
                        ElementIdentifier.of("INTERACTS_SEASON1"),
                        RelationshipProjection.builder()
                            .type("INTERACTS_SEASON1")
                            .orientation(Orientation.UNDIRECTED)
                            .build()
                    )).build())
                    .algo("gds", "alpha", "eigenvector")
            )
        );
    }

    private void createExplicitGraph(String graphName) {
        String graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Character")
            .withRelationshipType(
                "INTERACTS_SEASON1",
                RelationshipProjection.builder()
                    .type("INTERACTS_SEASON1")
                    .orientation(Orientation.UNDIRECTED)
                    .build()
            )
            .graphCreate(graphName)
            .yields();

        runQuery(graphCreateQuery);
    }
}
