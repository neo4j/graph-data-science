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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class SplitRelationshipsMutateProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE" +
                                            " (n0:A {id: 0})" +
                                            ",(n1:A {id: 1})" +
                                            ",(n2:A {id: 2})" +
                                            ",(n3:A {id: 3})" +
                                            ",(n4:A {id: 4})" +
                                            ",(n5:A {id: 5})" +
                                            ",(m0:B {id: 6})" +
                                            ",(m1:B {id: 7})" +
                                            ",(m2:B {id: 8})" +
                                            ",(m3:B {id: 9})" +
                                            ",(m4:B {id: 10})" +
                                            ",(m5:B {id: 11})" +
                                            ",(n0)-[:T {foo: 0}]->(n1)" +
                                            ",(n1)-[:T {foo: 1}]->(n2)" +
                                            ",(n2)-[:T {foo: 4}]->(n3)" +
                                            ",(n3)-[:T {foo: 9}]->(n4)" +
                                            ",(n4)-[:T {foo: 16}]->(n5)" +
                                            ",(m0)-[:T {foo: 0}]->(m1)" +
                                            ",(m1)-[:T {foo: 1}]->(m2)" +
                                            ",(m2)-[:T {foo: 4}]->(m3)" +
                                            ",(m3)-[:T {foo: 9}]->(m4)" +
                                            ",(m4)-[:T {foo: 16}]->(m5)";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(SplitRelationshipsMutateProc.class, GraphProjectProc.class);
        runQuery(DB_CYPHER);
        var createQuery = GdsCypher.call("graph")
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeProperty("id")
            .withRelationshipType("T", Orientation.UNDIRECTED)
            .withRelationshipProperty("foo")
            .yields();
        runQuery(createQuery);
    }

    @Test
    void shouldFailIfContainingIsMissing() {
        var query = GdsCypher.call("graph")
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
            .hasMessage("Could not find the specified `nonNegativeRelationshipTypes` of ['MISSING']. Available relationship types are ['T'].");
    }

    @Test
    void shouldFailIfRemainingExists() {
        var query = GdsCypher.call("graph")
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
            .hasMessage("The specified `remainingRelationshipType` of `T` already exists in the in-memory graph.");
    }

    @Test
    void shouldFailIfHoldoutExists() {

        var query = GdsCypher.call("graph")
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
            .hasMessage("The specified `holdoutRelationshipType` of `T` already exists in the in-memory graph.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"A", "B"})
    void shouldSplit(String nodeLabel) {
        var labelIdOffset = nodeLabel.equals("A") ? 0 : 6;
        var nodeId0 = 0 + labelIdOffset;
        var nodeId1 = 1 + labelIdOffset;
        var nodeId2 = 2 + labelIdOffset;
        var nodeId3 = 3 + labelIdOffset;
        var nodeId4 = 4 + labelIdOffset;
        var nodeId5 = 5 + labelIdOffset;
        var expectedTest = formatWithLocale(
                              " (a:" + nodeLabel + " {id: %d})" +
                              ",(b:" + nodeLabel + " {id: %d})" +
                              ",(c:" + nodeLabel + " {id: %d})" +
                              ",(d:" + nodeLabel + " {id: %d})" +
                              ",(e:" + nodeLabel + " {id: %d})" +
                              ",(f:" + nodeLabel + " {id: %d})" +
                              ",(d)-[:test {w: 0.0}]->(b)" +
                              ",(e)-[:test {w: 0.0}]->(c)" +
                              ",(c)-[:test {w: 1.0}]->(d)",
            nodeId0, nodeId1, nodeId2, nodeId3, nodeId4, nodeId5);
        var expectedTrain = formatWithLocale(
                               " (a:" + nodeLabel + " {id: %d})" +
                               ",(b:" + nodeLabel + " {id: %d})" +
                               ",(c:" + nodeLabel + " {id: %d})" +
                               ",(d:" + nodeLabel + " {id: %d})" +
                               ",(e:" + nodeLabel + " {id: %d})" +
                               ",(f:" + nodeLabel + " {id: %d})" +
                               ",(a)<-[:train]-(b)" +
                               ",(b)<-[:train]-(c)" +
                               ",(d)<-[:train]-(e)" +
                               ",(e)<-[:train]-(f)" +
                               ",(a)-[:train]->(b)" +
                               ",(b)-[:train]->(c)" +
                               ",(d)-[:train]->(e)" +
                               ",(e)-[:train]->(f)", nodeId0, nodeId1, nodeId2, nodeId3, nodeId4, nodeId5);
        var query = GdsCypher.call("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("sourceNodeLabels", List.of(nodeLabel))
            .addParameter("targetNodeLabels", List.of(nodeLabel))
            .addParameter("holdoutRelationshipType", "test")
            .addParameter("remainingRelationshipType", "train")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(query);

        var graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore();
        var testGraph = graphStore.getGraph(NodeLabel.of(nodeLabel), RelationshipType.of("test"), Optional.of(
            EdgeSplitter.RELATIONSHIP_PROPERTY));
        var trainGraph = graphStore.getGraph(NodeLabel.of(nodeLabel), RelationshipType.of("train"), Optional.empty());
        assertTrue(trainGraph.isUndirected());
        assertFalse(testGraph.isUndirected());
        TestSupport.assertGraphEquals(fromGdl(expectedTest), testGraph);
        TestSupport.assertGraphEquals(fromGdl(expectedTrain), trainGraph);
    }

    @ParameterizedTest
    @ValueSource(strings = {"A", "B"})
    void shouldSplitWithMasterGraph(String nodeLabel) {
        var labelIdOffset = nodeLabel.equals("A") ? 0 : 6;
        var nodeId0 = 0 + labelIdOffset;
        var nodeId1 = 1 + labelIdOffset;
        var nodeId2 = 2 + labelIdOffset;
        var nodeId3 = 3 + labelIdOffset;
        var nodeId4 = 4 + labelIdOffset;
        var nodeId5 = 5 + labelIdOffset;
        var expectedTest = formatWithLocale("CREATE" +
                              " (a:" + nodeLabel + " {id: %d})" +
                              ",(b:" + nodeLabel + " {id: %d})" +
                              ",(c:" + nodeLabel + " {id: %d})" +
                              ",(d:" + nodeLabel + " {id: %d})" +
                              ",(e:" + nodeLabel + " {id: %d})" +
                              ",(f:" + nodeLabel + " {id: %d})" +
                              ",(c)-[:innerTest {w: 0.0}]->(e)" +
                              ",(d)-[:innerTest {w: 0.0}]->(b)" +
                              ",(d)-[:innerTest {w: 1.0}]->(e)", nodeId0, nodeId1, nodeId2, nodeId3, nodeId4, nodeId5);
        var expectedTrain = formatWithLocale("CREATE" +
                               " (a:" + nodeLabel + "{id: %d})" +
                               ",(b:" + nodeLabel + "{id: %d})" +
                               ",(c:" + nodeLabel + "{id: %d})" +
                               ",(d:" + nodeLabel + "{id: %d})" +
                               ",(e:" + nodeLabel + "{id: %d})" +
                               ",(f:" + nodeLabel + "{id: %d})" +
                               ",(a)<-[:innerTrain]-(b)" +
                               ",(b)<-[:innerTrain]-(c)" +
                               ",(e)<-[:innerTrain]-(f)" +
                               ",(a)-[:innerTrain]->(b)" +
                               ",(b)-[:innerTrain]->(c)" +
                               ",(e)-[:innerTrain]->(f)", nodeId0, nodeId1, nodeId2, nodeId3, nodeId4, nodeId5);
        var outerSplitQuery = GdsCypher.call("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("sourceNodeLabels", List.of(nodeLabel))
            .addParameter("targetNodeLabels", List.of(nodeLabel))
            .addParameter("holdoutRelationshipType", "test")
            .addParameter("remainingRelationshipType", "train")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(outerSplitQuery);

        var innerSplitQuery = GdsCypher.call("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("sourceNodeLabels", List.of(nodeLabel))
            .addParameter("targetNodeLabels", List.of(nodeLabel))
            .addParameter("relationshipTypes", List.of("train"))
            .addParameter("nonNegativeRelationshipTypes", List.of("T"))
            .addParameter("holdoutRelationshipType", "innerTest")
            .addParameter("remainingRelationshipType", "innerTrain")
            .addParameter("holdoutFraction", 0.25)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(innerSplitQuery);

        var graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore();

        var testGraph = graphStore.getGraph(NodeLabel.of(nodeLabel), RelationshipType.of("innerTest"), Optional.of(EdgeSplitter.RELATIONSHIP_PROPERTY));
        var trainGraph = graphStore.getGraph(NodeLabel.of(nodeLabel), RelationshipType.of("innerTrain"), Optional.empty());
        assertTrue(trainGraph.isUndirected());
        assertFalse(testGraph.isUndirected());
        TestSupport.assertGraphEquals(fromGdl(expectedTest), testGraph);
        TestSupport.assertGraphEquals(fromGdl(expectedTrain), trainGraph);
    }

    @ParameterizedTest
    @ValueSource(strings = {"A", "B"})
    void shouldPreserveRelationshipWeights(String nodeLabel) {

        var query = GdsCypher.call("graph")
            .algo("gds.alpha.ml.splitRelationships")
            .mutateMode()
            .addParameter("sourceNodeLabels", List.of(nodeLabel))
            .addParameter("targetNodeLabels", List.of(nodeLabel))
            .addParameter("holdoutRelationshipType", "baz")
            .addParameter("remainingRelationshipType", "remaining")
            .addParameter("relationshipWeightProperty", "foo")
            .addParameter("holdoutFraction", 0.2)
            .addParameter("randomSeed", 1337L)
            .addParameter("negativeSamplingRatio", 2.0)
            .yields();
        runQuery(query);
        var graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), "graph").graphStore();
        var remainingGraph = graphStore.getGraph(NodeLabel.of(nodeLabel), RelationshipType.of("remaining"), Optional.of("foo"));
        remainingGraph.forEachNode(nodeId -> {
            remainingGraph.forEachRelationship(nodeId, Double.NaN, (s, t, w) -> {
                assertThat(w).isEqualTo(Math.pow(Math.min(s, t), 2));
                return true;
            });
            return true;
        });
    }
}
