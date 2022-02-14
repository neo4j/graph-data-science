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
package com.neo4j.gds.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphDropProc;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.RandomGraphTestCase;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.wcc.WccStreamProc;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.instanceOf;

class CypherAggregationTest extends BaseProcTest {

    @Neo4jGraph
    public static final String GRAPH =
        "CREATE" +
        " (:A)-[:REL]->(:A)<-[:REL]-(:A)" +
        ",(:A)-[:REL]->(:A)<-[:REL]-(:A)" +
        ",(:A)-[:REL]->(:DifferentLabel)" +
        ",(:A)-[:DISCONNECTED]->()" +

        ",(a:B { prop1: 42, prop2: 13.37, prop3: [42.0, 13.37], prop4: [13, 37] })" +
        ",(b:B { prop1: 43,               prop3: [44.0, 15.37], prop4: [14, 38] })" +
        ",(c:B { prop1: 44, prop2: 13.38, prop3: [45.0, 16.37]                  })" +
        ",(d:B { prop1: 45              , prop3: [43.0, 14.37], prop4: [16, 40] })" +
        ",(e:B { prop1: 46, prop2: 13.39                      , prop4: [17, 41] })" +
        ",(f:B { prop1: 47, prop2: 13.40, prop3: [46.0, 17.37], prop4: [18, 42] })" +

        ",(a)-[:REL {prop: 42} ]->(b)<-[:REL {prop: 43} ]-(c)" +
        ",(d)-[:REL {prop: 44} ]->(e)<-[:REL {prop: 45} ]-(f)";

    @BeforeEach
    void setup() throws Exception {
        registerAggregationFunctions(CypherAggregation.class);
        registerProcedures(GraphDropProc.class, GraphListProc.class, WccStreamProc.class);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testOptionalMatch() {
        assertCypherResult(
            "MATCH (s:A) OPTIONAL MATCH (s)-[:REL]->(t:A) WITH gds.alpha.graph('g', s, t) AS g RETURN" +
            " g.graphName AS graphName," +
            " g.projectMillis AS projectMillis," +
            " g.nodeCount AS nodeCount," +
            " g.relationshipCount AS relationshipCount",
            List.of(
                Map.of(
                    "graphName", "g",
                    "projectMillis", instanceOf(Long.class),
                    // all :A nodes
                    "nodeCount", 8L,
                    // only relationships between :A nodes
                    "relationshipCount", 4L
                )
            )
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore().getUnion();
        assertThat(graph)
            .returns(8L, Graph::nodeCount)
            .returns(4L, Graph::relationshipCount);
    }

    @Test
    void testMatch() {
        runQuery("MATCH (s:A)-[:REL]->(t:A) RETURN gds.alpha.graph('g', s, t)");

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore().getUnion();
        assertThat(graph)
            // only connected :A nodes
            .returns(6L, Graph::nodeCount)
            // only relationships between :A nodes
            .returns(4L, Graph::relationshipCount);
    }

    @Nested
    class LargerGraph extends RandomGraphTestCase {

        @Override
        @BeforeEach
        protected void setupGraph() {
            buildGraph(10000);
        }

        @Test
        void testLargerGraphSize() {
            runQuery("MATCH (s:Label) OPTIONAL MATCH (s)-[:TYPE]->(t:Label) RETURN gds.alpha.graph('g', s, t)");
            assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
            var graph = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore().getUnion();
            assertThat(graph.nodeCount()).isEqualTo(10000);
        }
    }

    @Test
    void testNodeMapping() {
        runQuery(
            "MATCH (s:B)-[:REL]->(t:B) RETURN " +
            "gds.alpha.graph('g', s, t)");

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore().getUnion();

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            for (char nodeVariable = 'a'; nodeVariable <= 'f'; nodeVariable++) {
                var neoNodeId = this.idFunction.of(String.valueOf(nodeVariable));
                var node = tx.getNodeById(neoNodeId);
                var nodeId = graph.toMappedNodeId(neoNodeId);

                assertThat(graph.toOriginalNodeId(nodeId)).isEqualTo(neoNodeId);
            }
        });
    }

    @Test
    void testNodeProperties() {
        runQuery(
            "MATCH (s:B)-[:REL]->(t:B) RETURN " +
            "gds.alpha.graph('g', s, t" +
            ", s {.prop1, another_prop: coalesce(s.prop2, 84.0), doubles: coalesce(s.prop3, [13.37, 42.0]), longs: coalesce(s.prop4, [42]) }" +
            ", t {.prop1, another_prop: coalesce(t.prop2, 84.0), doubles: coalesce(t.prop3, [13.37, 42.0]), longs: coalesce(t.prop4, [42]) }" +
            ")");

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore().getUnion();

        assertThat(graph.availableNodeProperties()).containsExactlyInAnyOrder(
            "prop1",
            "another_prop",
            "doubles",
            "longs"
        );
        var prop1 = graph.nodeProperties("prop1");
        var prop2 = graph.nodeProperties("another_prop");
        var prop3 = graph.nodeProperties("doubles");
        var prop4 = graph.nodeProperties("longs");

        assertThat(prop1.valueType()).isEqualTo(ValueType.LONG);
        assertThat(prop2.valueType()).isEqualTo(ValueType.DOUBLE);
        assertThat(prop3.valueType()).isEqualTo(ValueType.DOUBLE_ARRAY);
        assertThat(prop4.valueType()).isEqualTo(ValueType.LONG_ARRAY);

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            for (char nodeVariable = 'a'; nodeVariable <= 'f'; nodeVariable++) {
                var neoNodeId = this.idFunction.of(String.valueOf(nodeVariable));
                var node = tx.getNodeById(neoNodeId);
                var nodeId = graph.toMappedNodeId(neoNodeId);

                var expectedProp1 = node.getProperty("prop1");
                assertThat(prop1.longValue(nodeId)).isEqualTo(expectedProp1);

                var expectedProp2 = node.getProperty("prop2", 84.0);
                assertThat(prop2.doubleValue(nodeId)).isEqualTo(expectedProp2);

                var expectedProp3 = node.getProperty("prop3", new double[]{13.37, 42.0});
                assertThat(prop3.doubleArrayValue(nodeId)).containsExactly((double[]) expectedProp3);

                var expectedProp4 = node.getProperty("prop4", new long[]{42L});
                assertThat(prop4.longArrayValue(nodeId)).containsExactly((long[]) expectedProp4);
            }
        });
    }

    @Test
    void testSingleRelationshipProperties() {
        runQuery(
            "MATCH (s:B)-[r:REL]->(t:B) RETURN " +
            "gds.alpha.graph('g', s, t, null, null" +
            ", r {.prop}" +
            ")");

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore().getUnion();

        assertThat(graph.hasRelationshipProperty()).isTrue();

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            for (char nodeVariable = 'a'; nodeVariable <= 'f'; nodeVariable++) {
                var neoNodeId = this.idFunction.of(String.valueOf(nodeVariable));
                var node = tx.getNodeById(neoNodeId);

                var relationships = node.getRelationships(Direction.OUTGOING, RelationshipType.withName("REL"));
                var expectedProperties = StreamSupport
                    .stream(relationships.spliterator(), false)
                    .map(rel -> ((Number) rel.getProperty("prop")).doubleValue())
                    .collect(Collectors.toList());

                var nodeId = graph.toMappedNodeId(neoNodeId);
                graph.forEachRelationship(nodeId, Double.NaN, (s, t, property) -> {
                    assertThat(expectedProperties).contains(property);
                    expectedProperties.remove(property);
                    return true;
                });
                assertThat(expectedProperties)
                    .as("Not all properties were available in the graph, missing: %s", expectedProperties)
                    .isEmpty();
            }
        });
    }

    @Test
    void testMultipleRelationshipProperties() {
        runQuery(
            "MATCH (s:B)-[r:REL]->(t:B) RETURN " +
            "gds.alpha.graph('g', s, t, null, null" +
            ", r {.prop, prop_by_another_name: r.prop}" +
            ")");

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graphStore = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore();

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            for (var prop : List.of("prop", "prop_by_another_name")) {
                var graph = graphStore.getGraph(
                    org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS,
                    Optional.of(prop)
                );

                assertThat(graph.hasRelationshipProperty()).isTrue();

                for (char nodeVariable = 'a'; nodeVariable <= 'f'; nodeVariable++) {
                    var neoNodeId = this.idFunction.of(String.valueOf(nodeVariable));
                    var node = tx.getNodeById(neoNodeId);

                    var relationships = node.getRelationships(Direction.OUTGOING, RelationshipType.withName("REL"));
                    var expectedProperties = StreamSupport
                        .stream(relationships.spliterator(), false)
                        .map(rel -> ((Number) rel.getProperty("prop")).doubleValue())
                        .collect(Collectors.toList());

                    var nodeId = graph.toMappedNodeId(neoNodeId);
                    graph.forEachRelationship(nodeId, Double.NaN, (s, t, property) -> {
                        assertThat(expectedProperties).contains(property);
                        expectedProperties.remove(property);
                        return true;
                    });
                    assertThat(expectedProperties)
                        .as("Not all properties were available in the graph, missing: %s", expectedProperties)
                        .isEmpty();
                }
            }
        });
    }

    @Test
    void testRelationshipPropertiesAggregation() {
        runQuery("MATCH (a:B { prop1: 42 }), (b:B { prop1: 43 }) CREATE (a)-[:REL { prop:1337 }]->(b)");
        runQuery(
            "MATCH (s:B)-[r:REL]->(t:B) " +
            "WITH s, t, avg(r.prop) AS average, sum(r.prop) AS sum, max(r.prop) AS max, min(r.prop) AS min " +
            "RETURN gds.alpha.graph('g', s, t, null, null" +
            ", {average: average, sum: sum, max: max, min: min}" +
            ")");

        assertThat(GraphStoreCatalog.exists("", db.databaseId(), "g")).isTrue();
        var graphStore = GraphStoreCatalog.get("", db.databaseId(), "g").graphStore();

        GraphDatabaseApiProxy.runInTransaction(db, tx -> {
            var neoNodeId = this.idFunction.of("a");
            var node = tx.getNodeById(neoNodeId);

            Map.of(
                "average", (1337.0 + 42.0) / 2.0,
                "sum", 1337.0 + 42.0,
                "max", 1337.0,
                "min", 42.0
            ).forEach((prop, expectedValue) -> {
                var graph = graphStore.getGraph(
                    org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS,
                    Optional.of(prop)
                );

                assertThat(graph.hasRelationshipProperty()).isTrue();

                var nodeId = graph.toMappedNodeId(neoNodeId);
                assertThat(graph.degree(nodeId)).isEqualTo(1);
                graph.forEachRelationship(nodeId, Double.NaN, (s, t, property) -> {
                    assertThat(property).isEqualTo(expectedValue);
                    return true;
                });
            });

        });
    }


    @Test
    void testPipelinePseudoAnonymous() {
        assertCypherResult(
            "MATCH (s:A)-[:REL]->(t:A) " +
            "WITH gds.alpha.graph('g', s, t) AS g " +
            "CALL gds.wcc.stream(g.graphName) YIELD nodeId, componentId " +
            "RETURN count(DISTINCT componentId) as numberOfComponents",
            List.of(Map.of("numberOfComponents", 2L))
        );
    }
}
