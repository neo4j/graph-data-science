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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.centrality.EigenvectorCentralityProc;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateConfig;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.QueryRunner.runInTransaction;

class EigenvectorCentralityNormalizationProcTest extends BaseProcTest {

    private static final Map<Long, Double> noNormExpected = new HashMap<>();
    private static final Map<Long, Double> maxNormExpected = new HashMap<>();
    private static final Map<Long, Double> l2NormExpected = new HashMap<>();
    private static final Map<Long, Double> l1NormExpected = new HashMap<>();

    @AfterEach
    void tearDown() {
        GraphCatalog.removeAllLoadedGraphs();
        db.shutdown();
    }

    @BeforeEach
    void setup() throws Exception {
        ClassLoader classLoader = EigenvectorCentralityNormalizationProcTest.class.getClassLoader();
        File file = new File(classLoader.getResource("got/got-s1-nodes.csv").getFile());

        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
            .newImpermanentDatabaseBuilder(new File(UUID.randomUUID().toString()))
            .setConfig(GraphDatabaseSettings.load_csv_file_url_root, file.getParent())
            .setConfig(GraphDatabaseSettings.procedure_unrestricted, "algo.*")
            .newGraphDatabase();

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
    }

    @ParameterizedTest(name = "Normalizatoin: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityOnExplicitGraph(String normalizationType, Map<Long, Double> expected) {
        createExplicitGraph();
        String eigenvectorStreamQuery = GdsCypher.call()
            .explicitCreation("eigenvectorNorm")
            .algo("gds", "alpha", "eigenvector")
            .streamMode()
            .addParameter("direction", "BOTH")
            .addParameter("normalization", normalizationType)
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();

        runQuery(eigenvectorStreamQuery +
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

    @ParameterizedTest(name = "Normalizatoin: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityOnImplicitGraph(String normalizationType, Map<Long, Double> expected) {
        String query = GdsCypher.call().implicitCreation(ImmutableGraphCreateConfig
            .builder()
            .graphName("eigenvectorImplicitNorm")
            .nodeProjection(NodeProjections.of("Character"))
            .relationshipProjection(RelationshipProjections.single(
                ElementIdentifier.of("INTERACTS_SEASON1"),
                RelationshipProjection.builder()
                    .type("INTERACTS_SEASON1")
                    .projection(Projection.UNDIRECTED)
                    .build()
            )).build())
            .algo("gds", "alpha", "eigenvector")
            .streamMode()
            .addParameter("direction", "BOTH")
            .addParameter("normalization", normalizationType)
            .addParameter("writeProperty", "eigen")
            .yields("nodeId", "score");

        final Map<Long, Double> actual = new HashMap<>();

        runQuery(
            query +
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

    @ParameterizedTest(name = "Normalizatoin: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityWriteOnExplicitGraph(String normalizationType, Map<Long, Double> expected) {
        createExplicitGraph();
        final Map<Long, Double> actual = new HashMap<>();
        String eigenvectorWriteQuery = GdsCypher.call()
            .explicitCreation("eigenvectorNorm")
            .algo("gds", "alpha", "eigenvector")
            .writeMode()
            .addParameter("direction", "BOTH")
            .addParameter("normalization", normalizationType)
            .addParameter("writeProperty", "eigen")
            .yields();

        runQuery(eigenvectorWriteQuery);

        runQuery(
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

    @ParameterizedTest(name = "Normalizatoin: {0}")
    @MethodSource("normalizations")
    void eigenvectorCentralityWriteOnImplicitGraph(String normalizationType, Map<Long, Double> expected) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = GdsCypher.call().implicitCreation(ImmutableGraphCreateConfig
            .builder()
            .graphName("eigenvectorImplicitNorm")
            .nodeProjection(NodeProjections.of("Character"))
            .relationshipProjection(RelationshipProjections.single(
                ElementIdentifier.of("INTERACTS_SEASON1"),
                RelationshipProjection.builder()
                    .type("INTERACTS_SEASON1")
                    .projection(Projection.UNDIRECTED)
                    .build()
            )).build())
            .algo("gds", "alpha", "eigenvector")
            .writeMode()
            .addParameter("direction", "BOTH")
            .addParameter("normalization", normalizationType)
            .addParameter("writeProperty", "eigen")
            .yields();

        runQuery(query);

        runQuery(
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


    static Stream<Arguments> normalizations() {
        return Stream.of(
            arguments("none", noNormExpected),
            arguments("max", maxNormExpected),
            arguments("l1Norm", l1NormExpected),
            arguments("l2Norm", l2NormExpected)
        );
    }

    private void createExplicitGraph() {
        String graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Character")
            .withRelationshipType(
                "INTERACTS_SEASON1",
                RelationshipProjection.builder()
                    .type("INTERACTS_SEASON1")
                    .projection(Projection.UNDIRECTED)
                    .build()
            )
            .betaGraphCreate("eigenvectorNorm")
            .yields();
        runQuery(graphCreateQuery);
    }
}
