/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.ml.splitting;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class SplitRelationshipsMutateProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE" +
                                            " (n0:A {id: 0})" +
                                            ",(n1:A {id: 1})" +
                                            ",(n2:A {id: 2})" +
                                            ",(n3:A {id: 3})" +
                                            ",(n4:A {id: 4})" +
                                            ",(n5:A {id: 5})" +
                                            ",(n0)-[:T]->(n1)" +
                                            ",(n1)-[:T]->(n2)" +
                                            ",(n2)-[:T]->(n3)" +
                                            ",(n3)-[:T]->(n4)" +
                                            ",(n4)-[:T]->(n5)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SplitRelationshipsMutateProc.class, GraphCreateProc.class);
        runQuery(DB_CYPHER);
        var createQuery = GdsCypher.call()
            .withAnyLabel()
            .withNodeProperty("id")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .graphCreate("graph")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void shouldFailIfContainingIsMissing() {

        var query = GdsCypher.call()
            .explicitCreation("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("nonNegativeRelationshipTypes", List.of("MISSING"))
            .addParameter("holdoutRelationshipType", "test")
            .addParameter("remainingRelationshipType", "train")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("negativeSamplingRatio", 1.0)
            .yields();
        var ex = assertThrows(
            Exception.class,
            () -> runQuery(query)
        );
        assertThat(ex)
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(formatWithLocale(
                "Relationship type `%s` does not exist in the in-memory graph.",
                "MISSING"
            ));
    }

    @Test
    void shouldFailIfRemainingExists() {

        var query = GdsCypher.call()
            .explicitCreation("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("holdoutRelationshipType", "test")
            .addParameter("remainingRelationshipType", "T")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("negativeSamplingRatio", 1.0)
            .yields();
        var ex = assertThrows(
            Exception.class,
            () -> runQuery(query)
        );
        assertThat(ex)
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(formatWithLocale(
                "Relationship type `%s` already exists in the in-memory graph.",
                "T"
            ));
    }

    @Test
    void shouldFailIfHoldoutExists() {

        var query = GdsCypher.call()
            .explicitCreation("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("holdoutRelationshipType", "T")
            .addParameter("remainingRelationshipType", "train")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("negativeSamplingRatio", 1.0)
            .yields();
        var ex = assertThrows(
            Exception.class,
            () -> runQuery(query)
        );
        assertThat(ex)
            .getRootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(formatWithLocale(
                "Relationship type `%s` already exists in the in-memory graph.",
                "T"
            ));
    }

    @Test
    void shouldSplit() {
        var expectedTest = "CREATE" +
                              " (a {id: 0})" +
                              ",(b {id: 1})" +
                              ",(c {id: 2})" +
                              ",(d {id: 3})" +
                              ",(e {id: 4})" +
                              ",(f {id: 5})" +
                              ",(c)-[{w: 0.0}]->(f)" +
                              ",(e)-[{w: 0.0}]->(c)" +
                              ",(d)-[{w: 1.0}]->(c)";
        var expectedTrain = "CREATE" +
                               " (a {id: 0})" +
                               ",(b {id: 1})" +
                               ",(c {id: 2})" +
                               ",(d {id: 3})" +
                               ",(e {id: 4})" +
                               ",(f {id: 5})" +
                               ",(a)<-[]-(b)" +
                               ",(b)<-[]-(c)" +
                               ",(d)<-[]-(e)" +
                               ",(e)<-[]-(f)" +
                               ",(a)-[]->(b)" +
                               ",(b)-[]->(c)" +
                               ",(d)-[]->(e)" +
                               ",(e)-[]->(f)";
        var query = GdsCypher.call()
            .explicitCreation("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("holdoutRelationshipType", "test")
            .addParameter("remainingRelationshipType", "train")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(query);

        var graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), "graph").graphStore();
        var testGraph = graphStore.getGraph(RelationshipType.of("test"), Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY));
        var trainGraph = graphStore.getGraph(RelationshipType.of("train"));
        assertTrue(trainGraph.isUndirected());
        assertFalse(testGraph.isUndirected());
        TestSupport.assertGraphEquals(fromGdl(expectedTest), testGraph);
        TestSupport.assertGraphEquals(fromGdl(expectedTrain), trainGraph);
    }

    @Test
    void shouldSplitWithMasterGraph() {
        var expectedTest = "CREATE" +
                              " (a {id: 0})" +
                              ",(b {id: 1})" +
                              ",(c {id: 2})" +
                              ",(d {id: 3})" +
                              ",(e {id: 4})" +
                              ",(f {id: 5})" +
                              ",(c)-[{w: 0.0}]->(e)" +
                              ",(e)-[{w: 0.0}]->(c)" +
                              ",(d)-[{w: 1.0}]->(e)";
        var expectedTrain = "CREATE" +
                               " (a {id: 0})" +
                               ",(b {id: 1})" +
                               ",(c {id: 2})" +
                               ",(d {id: 3})" +
                               ",(e {id: 4})" +
                               ",(f {id: 5})" +
                               ",(a)<-[]-(b)" +
                               ",(b)<-[]-(c)" +
                               ",(e)<-[]-(f)" +
                               ",(a)-[]->(b)" +
                               ",(b)-[]->(c)" +
                               ",(e)-[]->(f)";
        var outerSplitQuery = GdsCypher.call()
            .explicitCreation("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("holdoutRelationshipType", "test")
            .addParameter("remainingRelationshipType", "train")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(outerSplitQuery);

        var innerSplitQuery = GdsCypher.call()
            .explicitCreation("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("relationshipTypes", List.of("train"))
            .addParameter("nonNegativeRelationshipTypes", List.of("T"))
            .addParameter("holdoutRelationshipType", "innerTest")
            .addParameter("remainingRelationshipType", "innerTrain")
            .addParameter("holdoutFraction", 0.25)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(innerSplitQuery);

        var graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), "graph").graphStore();
        var testGraph = graphStore.getGraph(RelationshipType.of("innerTest"), Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY));
        var trainGraph = graphStore.getGraph(RelationshipType.of("innerTrain"));
        assertTrue(trainGraph.isUndirected());
        assertFalse(testGraph.isUndirected());
        TestSupport.assertGraphEquals(fromGdl(expectedTest), testGraph);
        TestSupport.assertGraphEquals(fromGdl(expectedTrain), trainGraph);
    }
}
