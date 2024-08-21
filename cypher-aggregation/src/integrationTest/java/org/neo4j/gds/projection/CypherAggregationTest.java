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
package org.neo4j.gds.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.ResourceUtil;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.catalog.GraphDropProc;
import org.neo4j.gds.catalog.GraphListProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.RandomGraphTestCase;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.wcc.WccStreamProc;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.procedure.CallableUserAggregationFunction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.instanceOf;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class CypherAggregationTest extends BaseProcTest {

    @Neo4jGraph
    public static final String GRAPH = "CREATE" +
        " (:A)-[:REL]->(:A)<-[:REL]-(:A)" +
        ",(:A)-[:REL]->(:A)<-[:REL]-(:A)" +
        ",(:A)-[:REL]->(:DifferentLabel)" +
        ",(:A)-[:DISCONNECTED]->({ foo: 42 })" +

        ",(a:B { prop1: 42, prop2: 13.37, prop3: [42.0, 13.37], prop4: [13, 37] })" +
        ",(b:B { prop1: 43,               prop3: [44.0, 15.37], prop4: [14, 38] })" +
        ",(c:B { prop1: 44, prop2: 13.38, prop3: [45.0, 16.37]                  })" +
        ",(d:B { prop1: 45              , prop3: [43.0, 14.37], prop4: [16, 40] })" +
        ",(e:B { prop1: 46, prop2: 13.39                      , prop4: [17, 41] })" +
        ",(f:B { prop1: 47, prop2: 13.40, prop3: [46.0, 17.37], prop4: [18, 42] })" +

        ",(a)-[:REL {prop: 42} ]->(b)<-[:REL {prop: 43} ]-(c)" +
        ",(d)-[:REL {prop: 44} ]->(e)<-[:REL]-(f)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerUserAggregationFunction(db, new CypherAggregation());
        registerProcedures(GraphDropProc.class, GraphListProc.class, WccStreamProc.class);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    private static void registerUserAggregationFunction(
        GraphDatabaseService db,
        CallableUserAggregationFunction function
    )
        throws KernelException {
        var globalProcedures = GraphDatabaseApiProxy.resolveDependency(db, GlobalProcedures.class);
        var alreadyExists = Neo4jProxy.globalProcedureRegistry(globalProcedures)
            .getAllAggregatingFunctions()
            .anyMatch(e -> e.equals(function.signature()));

        if (!alreadyExists) {
            GraphDatabaseApiProxy.register(db, function);
        }
    }

    @Test
    void testMatch() {
        runQuery("MATCH (s:A)-[:REL]->(t:A) RETURN gds.graph.project('g', s, t)");

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();
        assertThat(graph)
            // only connected :A nodes
            .returns(6L, Graph::nodeCount)
            // only relationships between :A nodes
            .returns(4L, Graph::relationshipCount);
    }

    @Test
    void testArbitraryIds() {
        assertCypherResult(
            "UNWIND(range(13, 37)) AS source " +
                "WITH source, source + 42 AS target " +
                "WITH gds.graph.project('g', source, target) AS g " +
                "RETURN g.nodeCount AS nodes, g.relationshipCount AS rels",
            List.of(Map.of("nodes", (37L - 13L + 1L) * 2L, "rels", 37L - 13L + 1L))
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();

        for (long source = 13L; source < 37; source++) {
            long sourceNodeId = graph.toMappedNodeId(source);
            long expectedOriginalId = (source - 13L) * 2L;
            long expectedTargetId = source + 42L;
            assertThat(sourceNodeId).isEqualTo(expectedOriginalId);
            graph.forEachRelationship(sourceNodeId, (s, targetNodeId) -> {
                assertThat(targetNodeId).isEqualTo(expectedOriginalId + 1L);
                long target = graph.toOriginalNodeId(targetNodeId);
                assertThat(target).isEqualTo(expectedTargetId);
                return true;
            });
        }
    }

    @Test
    void testOptionalMatch() {
        assertCypherResult(
            "MATCH (s:A) OPTIONAL MATCH (s)-[:REL]->(t:A) WITH gds.graph.project('g', s, t) AS g RETURN" +
                " g.graphName AS graphName," +
                " g.projectMillis AS projectMillis," +
                " g.nodeCount AS nodeCount," +
                " g.relationshipCount AS relationshipCount",
            List.of(
                Map.of(
                    "graphName",
                    "g",
                    "projectMillis",
                    instanceOf(Long.class),
                    // all :A nodes
                    "nodeCount",
                    8L,
                    // only relationships between :A nodes
                    "relationshipCount",
                    4L
                )
            )
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();
        assertThat(graph)
            .returns(8L, Graph::nodeCount)
            .returns(4L, Graph::relationshipCount);
    }

    @Test
    void testDifferentPropertySchemas() {
        var query = "UNWIND [" +
            "  [0, 1, 'a', {}, 'rel', {}], " +
            "  [2, 3, 'b', {x:1}, 'rel2', {weight: 0.1}]," +
            "  [5, 6, 'c', {y:1}, 'rel3', {hq: 0.1}]" +
            "] AS data" +
            " RETURN gds.graph.project(" +
            "    'g'," +
            "    data[0]," +
            "    data[1]," +
            "    {" +
            "        sourceNodeLabels: data[2]," +
            "        targetNodeLabels: NULL," +
            "        sourceNodeProperties: data[3]," +
            "        targetNodeProperties: NULL," +
            "        relationshipType: data[4]," +
            "        relationshipProperties: data[5]" +
            "    }" +
            ")";

        runQuery(query);

        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();

        assertThat(graph.schema().nodeSchema().get(NodeLabel.of("a")).properties().keySet()).isEmpty();
        assertThat(graph.schema().nodeSchema().get(NodeLabel.of("b")).properties().keySet()).containsExactly("x");
        assertThat(graph.schema().nodeSchema().get(NodeLabel.of("c")).properties().keySet()).containsExactly("y");

        assertThat(
            graph
                .schema()
                .relationshipSchema()
                .get(org.neo4j.gds.RelationshipType.of("rel"))
                .properties()
                .keySet()
        ).isEmpty();
        assertThat(
            graph
                .schema()
                .relationshipSchema()
                .get(org.neo4j.gds.RelationshipType.of("rel2"))
                .properties()
                .keySet()
        ).containsExactly("weight");
        assertThat(
            graph
                .schema()
                .relationshipSchema()
                .get(org.neo4j.gds.RelationshipType.of("rel3"))
                .properties()
                .keySet()
        ).containsExactly("hq");
    }

    @Test
    void testInvalidNegativId() {
        assertThatThrownBy(() -> runQuery("RETURN gds.graph.project('g', -1)"))
            .rootCause()
            .hasMessage("GDS expects node ids to be positive. But got a negative id of `-1`.");
    }

    @Test
    void testInvalidRelationshipAsArbitraryId() {
        var query = "MATCH ()-[r]-() RETURN gds.graph.project('g', r)";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage("The node has to be either a NODE or an INTEGER, but got RelationshipReference");
    }

    @Test
    void testInvalidPathAsArbitraryId() {
        var query = "MATCH p=()-[]-() RETURN gds.graph.project('g', p)";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage("The node has to be either a NODE or an INTEGER, but got Path");
    }

    @ParameterizedTest
    @CsvSource({
        "13.37, Double",
        "true, Boolean",
        "false, Boolean",
        "null, NO_VALUE",
        "\"42\", String",
        "[42], List",
        "[13.37], List",
        "{foo:42}, Map",
        "{foo:13.37}, Map"})
    void testInvalidArbitraryIds(String idLiteral, String invalidType) {
        var query = formatWithLocale(
            "WITH %s AS source RETURN gds.graph.project('g', source)",
            idLiteral
        );
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage("The node has to be either a NODE or an INTEGER, but got " + invalidType);
    }

    @Nested
    class CoraGraphTest extends BaseTest {
        @Test
        void load_cora_rels() {
            List<String> lines = ResourceUtil.lines("cora_rels.csv");
            List<long[]> rows = lines.stream().map(i -> {
                var values = i.split(",");
                return new long[]{Long.parseLong(values[0]), Long.parseLong(values[1])};
            }).collect(Collectors.toList());

            runQuery(
                "UNWIND $data AS data WITH data RETURN gds.graph.project('g', data[0], data[1], {}, {readConcurrency: 1})",
                Map.of("data", rows)
            );

            assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
            var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();
            assertThat(graph.nodeCount()).isEqualTo(2708);
            assertThat(graph.relationshipCount()).isEqualTo(rows.size());
        }
    }

    @Test
    void testNodeMapping() {
        runQuery(
            "MATCH (s:B)-[:REL]->(t:B) RETURN " +
                "gds.graph.project('g', s, t)"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();

        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
            for (char nodeVariable = 'a'; nodeVariable <= 'f'; nodeVariable++) {
                var neoNodeId = this.idFunction.of(String.valueOf(nodeVariable));
                var node = tx.getNodeById(neoNodeId);
                var nodeId = graph.toMappedNodeId(neoNodeId);

                assertThat(graph.toOriginalNodeId(nodeId)).isEqualTo(neoNodeId);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"labels(s)", "['A', 'B']"})
    void testNodeLabels(String labels) {
        runQuery(
            "MATCH (s) WHERE s:A or s:B " +
                "RETURN gds.graph.project('g', s, null, { sourceNodeLabels: " +
                labels +
                ", targetNodeLabels: NULL })"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();

        assertThat(graphStore.relationshipTypes())
            .containsExactlyInAnyOrderElementsOf(graphStore.schema().relationshipSchema().availableTypes());

        assertThat(graphStore.nodeLabels()).extracting(NodeLabel::name).containsExactly("A", "B");
    }

    @Test
    void testEmptyNodeLabel() {
        runQuery(
            "MATCH (s)" +
                "RETURN gds.graph.project('g', s, null, { sourceNodeLabels: labels(s), targetNodeLabels: NULL })"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        assertThat(graphStore.nodeCount()).isEqualTo(16);
        assertThat(graphStore.nodeLabels())
            .extracting(NodeLabel::name)
            .containsExactly("A", "B", "__ALL__", "DifferentLabel");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
            "",
            ", {}",
            ", { sourceNodeLabels: false, targetNodeLabels: NULL }"
        })
    void testWithoutLabelInformation(String nodeConfig) {
        runQuery(
            "MATCH (s)" +
                "RETURN gds.graph.project('g', s, null" +
                nodeConfig +
                ")"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        assertThat(graphStore.nodeLabels()).extracting(NodeLabel::name).containsExactly("__ALL__");
    }

    @Test
    void testInvalidDirectLabelMapping() {
        var query = "MATCH (s) RETURN gds.graph.project('g', s, null, { sourceNodeLabels: true, targetNodeLabels: NULL })";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Using `true` to load all labels is deprecated, use `{ sourceNodeLabels: labels(s) }` instead");
    }

    @ParameterizedTest
    @ValueSource(strings = {"labels(s)[0]", "'A'"})
    void testSingleNodeLabel(String label) {
        runQuery(
            "MATCH (s:A)" +
                "RETURN gds.graph.project('g', s, null, { sourceNodeLabels: " +
                label +
                ", targetNodeLabels: NULL })"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        assertThat(graphStore.nodeLabels()).extracting(NodeLabel::name).containsExactly("A");
    }

    @Test
    void testPropertiesOnEmptyNodes() {
        runQuery(
            "MATCH (s)" +
                "RETURN gds.graph.project('g', s, null, { sourceNodeProperties: s { .foo }, targetNodeProperties: NULL })"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        var graph = graphStore.getUnion();
        var nodeProperties = graph.nodeProperties("foo");

        var nonDefaultProperties = LongStream.range(0, graph.nodeCount())
            .map(nodeProperties::longValue)
            .filter(prop -> prop != DefaultValue.forLong().longValue())
            .toArray();

        assertThat(nonDefaultProperties).containsExactly(42);
    }

    @Test
    void testLabelsOnNodeWithoutLabel() {
        runQuery(
            "UNWIND [[0, []], [1, ['Label']]] AS idAndLabels " +
                "WITH idAndLabels[0] AS id, idAndLabels[1] AS labels " +
                "RETURN gds.graph.project('g', id, null, { sourceNodeLabels: labels, targetNodeLabels: NULL })"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        var graph = graphStore.getGraph(NodeLabel.of("Label"));

        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    @Test
    void testNodeProperties() {
        runQuery(
            "MATCH (s:B)-[:REL]->(t:B) RETURN " +
                "gds.graph.project('g', s, t, {" +
                "   sourceNodeProperties: s {.prop1, another_prop: coalesce(s.prop2, 84.0), doubles: coalesce(s.prop3, [13.37, 42.0]), longs: coalesce(s.prop4, [42]) }," +
                "   targetNodeProperties: t {.prop1, another_prop: coalesce(t.prop2, 84.0), doubles: coalesce(t.prop3, [13.37, 42.0]), longs: coalesce(t.prop4, [42]) }" +
                "})"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();

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

        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
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

    @ParameterizedTest
    @ValueSource(strings = {"type(r)", "'REL'"})
    void testRelationshipType(String type) {
        runQuery(
            "MATCH (s:B)-[r:REL]->(t:B) RETURN " +
                "gds.graph.project('g', s, t," +
                "   { relationshipType: " +
                type +
                " }" +
                ")"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g")
            .graphStore()
            .getGraph(org.neo4j.gds.RelationshipType.of("REL"));

        assertThat(graph)
            .returns(6L, Graph::nodeCount)
            .returns(4L, Graph::relationshipCount);
    }

    @Test
    void testSingleRelationshipProperties() {
        runQuery(
            "MATCH (s:B)-[r:REL]->(t:B) RETURN " +
                "gds.graph.project('g', s, t," +
                "   { relationshipProperties: r {.prop} }" +
                ")"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();

        assertThat(graph.hasRelationshipProperty()).isTrue();
        assertThat(graph.relationshipCount()).isEqualTo(4);

        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
            for (char nodeVariable = 'a'; nodeVariable <= 'f'; nodeVariable++) {
                var neoNodeId = this.idFunction.of(String.valueOf(nodeVariable));
                var node = tx.getNodeById(neoNodeId);

                var relationships = node.getRelationships(Direction.OUTGOING, RelationshipType.withName("REL"));
                var expectedProperties = StreamSupport
                    .stream(relationships.spliterator(), false)
                    .map(rel -> ((Number) rel.getProperty("prop", Double.NaN)).doubleValue())
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
    void ignoreRelationshipTypeIfTargetIsNull() {
        runQuery(
            "UNWIND [[1, null, null], [1, 2, 'REL']] AS data" +
                " RETURN gds.graph.project('g', data[0], data[1], {relationshipType: data[2]})"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();

        assertThat(graphStore.relationshipCount()).isEqualTo(1);
        assertThat(graphStore.relationshipTypes()).containsOnly(org.neo4j.gds.RelationshipType.of("REL"));
    }

    @Test
    void shouldNotFailOnMissingProperty() {
        var query = "MATCH (s:B)-[:REL]->(t:B) " +
            "WITH gds.graph.project('g', s, t, {" +
            "   sourceNodeProperties: s { .prop2 }, " +
            "   targetNodeProperties: t { .prop2 }" +
            "}) AS g " +
            "RETURN g.graphName";
        assertThatCode(() -> runQuery(query)).doesNotThrowAnyException();
    }

    @Test
    void testMultipleRelationshipProperties() {
        runQuery(
            "MATCH (s:B)-[r:REL]->(t:B) RETURN " +
                "gds.graph.project('g', s, t, " +
                "   { relationshipProperties: r {.prop, prop_by_another_name: r.prop} }" +
                ")"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();

        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
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
                        .map(rel -> ((Number) rel.getProperty("prop", Double.NaN)).doubleValue())
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
            "MATCH (s:B { prop1: 42 })-[r:REL]->(t:B { prop1: 43 }) " +
                "WITH s, t, avg(r.prop) AS average, sum(r.prop) AS sum, max(r.prop) AS max, min(r.prop) AS min " +
                "RETURN gds.graph.project('g', s, t," +
                "   { relationshipProperties: {average: average, sum: sum, max: max, min: min} }" +
                ")"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();

        assertThat(graphStore)
            .returns(2L, GraphStore::nodeCount)
            .returns(1L, GraphStore::relationshipCount);

        GraphDatabaseApiProxy.runInFullAccessTransaction(db, tx -> {
            var neoNodeId = this.idFunction.of("a");
            var node = tx.getNodeById(neoNodeId);

            Map.of(
                "average",
                (1337.0 + 42.0) / 2.0,
                "sum",
                1337.0 + 42.0,
                "max",
                1337.0,
                "min",
                42.0
            ).forEach((prop, expectedValue) -> {
                var graph = graphStore.getGraph(
                    org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS,
                    Optional.of(prop)
                );

                assertThat(graph.hasRelationshipProperty()).isTrue();

                var nodeId = graph.toMappedNodeId(neoNodeId);
                assertThat(graph.degree(nodeId)).isEqualTo(1);
                graph.forEachRelationship(nodeId, Double.NaN, (s, t, property) -> {
                    assertThat(property)
                        .as("source %d target %d key %s value %f", s, t, prop, property)
                        .isEqualTo(expectedValue);
                    return true;
                });
            });

        });
    }

    @ParameterizedTest
    @MethodSource("undirectedTypes")
    void testRespectUndirectedTypes(List<String> undirectedConfig, List<String> expectedUndirectedTypes) {
        runQuery(
            "UNWIND [" +
                " {s: 0, t: 1, type: 'UNDIRECTED'}, " +
                " {s: 1, t: 2, type: 'DIRECTED'} " +
                "] as d" +
                " RETURN gds.graph.project('g', d.s, d.t, {relationshipType: d.type}, {undirectedRelationshipTypes: $undirected})",
            Map.of("undirected", undirectedConfig)
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();


        graphStore
            .schema()
            .relationshipSchema()
            .entries()
            .forEach(entry -> {
                var expectedDirection = expectedUndirectedTypes.contains(entry.identifier().name)
                    ? org.neo4j.gds.api.schema.Direction.UNDIRECTED
                    : org.neo4j.gds.api.schema.Direction.DIRECTED;
                assertThat(entry.direction()).isEqualTo(expectedDirection);
            });
    }

    @Test
    void testRespectInverseIndexedRelTypes() {
        runQuery(
            "UNWIND [" +
                " {s: 0, t: 1, type: 'INDEXED'}, " +
                " {s: 1, t: 2, type: 'REL'} " +
                "] as d" +
                " RETURN gds.graph.project('g', d.s, d.t, {relationshipType: d.type}, {inverseIndexedRelationshipTypes: ['INDEXED']})"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();

        assertThat(graphStore.inverseIndexedRelationshipTypes())
            .containsExactlyInAnyOrder(org.neo4j.gds.RelationshipType.of("INDEXED"));
    }

    private static Stream<Arguments> undirectedTypes() {
        return Stream.of(
            Arguments.of(
                List.of("UNDIRECTED"),
                List.of("UNDIRECTED")
            ),
            Arguments.of(
                List.of("UNDIRECTED", "DIRECTED"),
                List.of("UNDIRECTED", "DIRECTED")
            ),
            Arguments.of(
                List.of("*"),
                List.of("UNDIRECTED", "DIRECTED")
            )
        );
    }

    @ParameterizedTest
    @CsvSource({"42, Long", "13.37, Double"})
    void testInvalidLabel(String label, String type) {
        assertThatThrownBy(
            () -> runQuery(
                "MATCH (s) RETURN gds.graph.project('g', s, null, { sourceNodeLabels: " + label +
                    ", targetNodeLabels: NULL })"
            )
        )
            .rootCause()
            .hasMessage(
                "The value of `sourceNodeLabels` must be either a `List of Strings`, a `String`, or a `Boolean`, but was `" +
                    type +
                    "`."
            );
    }

    @Test
    void writeOperationsBlockedWhenPassingArbitraryIdsAsInput() {
        runQuery(
            "UNWIND [ [0, 1], [0, 2] ] as d " +
                "RETURN gds.graph.project('g', d[0], d[1])"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        assertThat(graphStore.capabilities().canWriteToLocalDatabase()).isFalse();
    }

    @Test
    void writeOperationsAllowedWhenPassingArbitraryIdsAsInput() {
        runQuery(
            "MATCH (s:A)-[:REL]->(t:A) " +
                "RETURN gds.graph.project('g', s, t)"
        );

        var graphStore = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore();
        assertThat(graphStore.capabilities().canWriteToLocalDatabase()).isTrue();
    }

    @Test
    void testPipelinePseudoAnonymous() {
        assertCypherResult(
            "MATCH (s:A)-[:REL]->(t:A) " +
                "WITH gds.graph.project('g', s, t) AS g " +
                "CALL gds.wcc.stream(g.graphName) YIELD nodeId, componentId " +
                "RETURN count(DISTINCT componentId) as numberOfComponents",
            List.of(Map.of("numberOfComponents", 2L))
        );
    }


    @Test
    void testReturnedConfig() {
        var query = "MATCH (a:A)-[:REL]->(b) " +
            "WITH gds.graph.project('g', a, b) AS g " +
            "RETURN g.configuration AS config";
        assertCypherResult(
            query,
            List.of(
                Map.of(
                    "config",
                    Map.of(
                        "inverseIndexedRelationshipTypes",
                        List.of(),
                        "jobId",
                        any(String.class),
                        "logProgress",
                        true,
                        "query",
                        query,
                        "readConcurrency",
                        4,
                        "undirectedRelationshipTypes",
                        List.of()
                    )
                )
            )
        );
    }

    @Test
    void testAlphaForwarding() {
        runQuery(
            "MATCH (s:B)-[r:REL]-(t:B) " +
                "RETURN gds.alpha.graph.project('g1', s, t," +
                "  { sourceNodeLabels: labels(s), targetNodeLabels: labels(t)," +
                "    sourceNodeProperties: s { .prop1 }, targetNodeProperties: t { .prop1 } }," +
                "  { relationshipType: type(r), properties: r { .prop } }," +
                "  { undirectedRelationshipTypes: ['REL'] }" +
                ");"
        );

        runQuery(
            "MATCH (s:B)-[r:REL]-(t:B) " +
                "RETURN gds.graph.project('g2', s, t," +
                "  { sourceNodeLabels: labels(s), targetNodeLabels: labels(t)," +
                "    sourceNodeProperties: s { .prop1 }, targetNodeProperties: t { .prop1 }," +
                "    relationshipType: type(r), relationshipProperties: r { .prop } }," +
                "  { undirectedRelationshipTypes: ['REL'] }" +
                ");"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g1")).isTrue();
        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g2")).isTrue();
        var g1 = GraphStoreCatalog.get("", db.databaseName(), "g1").graphStore().getUnion();
        var g2 = GraphStoreCatalog.get("", db.databaseName(), "g2").graphStore().getUnion();

        TestSupport.assertGraphEquals(g1, g2);
    }

    @Test
    void testAlphaForwardingWithMissingTargetConfigKeys() {
        runQuery(
            "MATCH (s:B)-[r:REL]-(t:B) " +
                "RETURN gds.alpha.graph.project('g1', s, t," +
                "  { sourceNodeLabels: labels(s)," +
                "    targetNodeProperties: t { .prop1 } }," +
                "  NULL," +
                "  {}" +
                ");"
        );

        runQuery(
            "MATCH (s:B)-[r:REL]-(t:B) " +
                "RETURN gds.graph.project('g2', s, t," +
                "  { sourceNodeLabels: labels(s), targetNodeLabels: NULL," +
                "    sourceNodeProperties: NULL, targetNodeProperties: t { .prop1 } }" +
                ");"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g1")).isTrue();
        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g2")).isTrue();
        var g1 = GraphStoreCatalog.get("", db.databaseName(), "g1").graphStore().getUnion();
        var g2 = GraphStoreCatalog.get("", db.databaseName(), "g2").graphStore().getUnion();

        TestSupport.assertGraphEquals(g1, g2);
    }

    @Test
    void testAlphaForwardingWithMissingSourceConfigKeys() {
        runQuery(
            "MATCH (s:B)-[r:REL]-(t:B) " +
                "RETURN gds.alpha.graph.project('g1', s, t," +
                "  { targetNodeLabels: labels(s)," +
                "    sourceNodeProperties: t { .prop1 } }," +
                "  NULL," +
                "  {}" +
                ");"
        );

        runQuery(
            "MATCH (s:B)-[r:REL]-(t:B) " +
                "RETURN gds.graph.project('g2', s, t," +
                "  { targetNodeLabels: labels(s), sourceNodeLabels: NULL," +
                "    targetNodeProperties: NULL, sourceNodeProperties: t { .prop1 } }" +
                ");"
        );

        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g1")).isTrue();
        assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g2")).isTrue();
        var g1 = GraphStoreCatalog.get("", db.databaseName(), "g1").graphStore().getUnion();
        var g2 = GraphStoreCatalog.get("", db.databaseName(), "g2").graphStore().getUnion();

        TestSupport.assertGraphEquals(g1, g2);
    }

    @ParameterizedTest
    @CsvSource({"42, Long", "13.37, Double", "true, Boolean", "\"A\", String"})
    void testInvalidProperties(String properties, String type) {
        assertThatThrownBy(
            () -> runQuery(
                "MATCH (s) RETURN gds.graph.project('g', s, null, { sourceNodeProperties: " + properties +
                    ", targetNodeProperties: NULL })"
            )
        )
            .rootCause()
            .hasMessage(
                "The value of `sourceNodeProperties` must be a `Map of Property Values`, but was `" + type + "`."
            );
    }

    @Test
    void testParsingAnInvalidConfig() {
        assertThatThrownBy(
            () -> runQuery(
                "MATCH (s) RETURN gds.graph.project('g', s, null, {}, {readConcurrency: 80})"
            )
        )
            .rootCause()
            .hasMessageContaining("80")
            .hasMessageContaining("readConcurrency");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
            "relationshipType: 'REL'",
            "relationshipProperties: r { .prop }",
            // does the validation also when the old key is present
            "properties: r { .prop }",
        })
    void testSplitMapsValidation(String relationshipConfig) {
        var query = "MATCH (s)-[r]-(t) RETURN gds.graph.project('g', s, null, {}, {" + relationshipConfig + "})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "The parameters for `nodesConfig` and `relationshipsConfig` have been merged. " +
                    "Update your query by merging the 4th and 5th parameter into one parameter."
            );
    }

    @SuppressWarnings("unchecked")
    @Test
    void testReadConcurrencyIsParsedCorrectly() {
        var configMap = (Map<String, Object>) runQuery(
            "MATCH (s)" +
                "RETURN gds.graph.project('g', s, null, {}, {readConcurrency: 2}).configuration as config",
            (Result result) -> result.next().get("config")
        );

        var config = GraphProjectFromCypherAggregationConfig.of("", "g", "", configMap);

        assertThat(config.readConcurrency()).isEqualTo(new Concurrency(2));
    }

    @Test
    void testReturnExecutingQuery() {
        var query = "MATCH (s)-->(t) RETURN gds.graph.project('g', s, t, {}).query as query";
        var resultQuery = runQuery(query, result -> result.<String>columnAs("query").next());

        assertThat(resultQuery).isEqualTo(query);

        var listQuery = runQuery(
            "CALL gds.graph.list('g') YIELD configuration RETURN configuration.query as query",
            result -> result.<String>columnAs("query").next()
        );

        assertThat(listQuery).isEqualTo(query);
    }

    @Test
    void testReturnExecutingQueryWithParams() {
        var query = "MATCH (s)-->(t) RETURN gds.graph.project($graphName, s, t, {}).query as query";
        var resultQuery = runQuery(query, Map.of("graphName", "g"), result -> result.<String>columnAs("query").next());

        assertThat(resultQuery).isEqualTo(query);

        var listQuery = runQuery(
            "CALL gds.graph.list('g') YIELD configuration RETURN configuration.query as query",
            result -> result.<String>columnAs("query").next()
        );

        assertThat(listQuery).isEqualTo(query);
    }

    @Test
    void testUnknownNodeConfigKeys() {
        var query = "MATCH (s) RETURN gds.graph.project('g', s, null, {foo: 'bar'})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage("Unexpected configuration key: foo");
    }

    @Test
    void testUnknownNodeConfigKeysWithSuggestion() {
        var query = "MATCH (s) RETURN gds.graph.project('g', s, null, {sourceNode: 'bar'})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "Unexpected configuration key: sourceNode (Did you mean one of [sourceNodeLabels, sourceNodeProperties]?)"
            );
    }

    @Test
    void testUnknownRelationshipConfigKeysWithSuggestion() {
        var query = "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {relationshipProperty: 'bar'})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "Unexpected configuration key: relationshipProperty " +
                    "(Did you mean one of [relationshipProperties, relationshipType]?)"
            );
    }

    @Test
    void testMigrationNoteForOldPropertiesKey() {
        var query = "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {properties: 'bar'})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "The configuration key 'properties' is now called 'relationshipProperties'."
            );
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
            "readConcurrency: 2",
            ""
        })
    void testSplitMapsConfigurationValidation(String configuration) {
        var query = "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {}, {}, {" + configuration + "})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "The parameters for `nodesConfig` and `relationshipsConfig` have been merged. " +
                    "Update your query by merging the 4th and 5th parameter into one parameter."
            );
    }

    @ParameterizedTest
    @CsvSource({
        "sourceNodeLabels, labels(s), targetNodeLabels",
        "sourceNodeProperties, s { .prop1 }, targetNodeProperties",
        "targetNodeLabels, labels(t), sourceNodeLabels",
        "targetNodeProperties, t { .prop1 }, sourceNodeProperties",
    })
    void testMissingSourceOrTargetNodeInformation(String presentKey, String value, String missingKey) {
        var query = formatWithLocale(
            "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {%s: %s})",
            presentKey,
            value
        );
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                formatWithLocale(
                    "The configuration key '%1$s' is missing, but '%2$s' is provided. " +
                        "If you really meant to only provide `%2$s` with no value for `%1$s`, " +
                        "you can set `%1$s` to `NULL`.",
                    missingKey,
                    presentKey
                )
            );
    }

    @Test
    void testSwappedConfigurationMaps() {
        var query = "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {readConcurrency: 2}, {relationshipType: 'REL'})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "The configuration parameters are provided in the wrong order. " +
                    "Update your query by swapping the 4th and 5th parameter."
            );
    }

    @Test
    void testMergedConfigurationMaps() {
        var query = "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {relationshipType: 'REL', readConcurrency: 2})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "The configuration parameters are merged and provided as one parameter. " +
                    "Update your query by splitting the configuration into two parameters. " +
                    "Refer to the documentation for details."
            );
    }

    @Test
    void testMissingDataConfig() {
        var query = "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {readConcurrency: 2})";
        assertThatThrownBy(() -> runQuery(query))
            .rootCause()
            .hasMessage(
                "The `dataConfig` configuration parameter is missing. " +
                    "If you meant to provide an empty configuration for the 4th parameter, " +
                    "you can pass an empty map: '{}'."
            );
    }

    @ParameterizedTest
    @CsvSource({
        "sourceNodeLabels, labels(s), targetNodeLabels",
        "sourceNodeProperties, s { .prop1 }, targetNodeProperties",
        "targetNodeLabels, labels(t), sourceNodeLabels",
        "targetNodeProperties, t { .prop1 }, sourceNodeProperties",
    })
    void testExplicitlyMissingSourceOrTargetNodeInformation(String presentKey, String value, String missingKey) {
        var query = formatWithLocale(
            "MATCH (s)--(t) RETURN gds.graph.project('g', s, null, {%s: %s, %s: NULL})",
            presentKey,
            value,
            missingKey
        );
        assertThatCode(() -> runQuery(query))
            .doesNotThrowAnyException();
    }

    @Test
    void shouldFailOnEmptyGraphName() {
        var query =
            """
                UNWIND [
                  [0, 1, 'a', {}, 'rel', {}],\s
                  [2, 3, 'b', {x:1}, 'rel2', {weight: 0.1}],
                  [5, 6, 'c', {y:1}, 'rel3', {hq: 0.1}]
                ] AS data
                 RETURN gds.graph.project(
                    '',
                    data[0],
                    data[1],
                    {
                        sourceNodeLabels: data[2],
                        targetNodeLabels: null,
                        sourceNodeProperties: data[3],
                        targetNodeProperties: null,
                        relationshipType: data[4],
                        relationshipProperties: data[5]
                    }
                )
                """;

        assertThatException()
            .isThrownBy(() -> runQuery(query))
            .withMessageContaining("`graphName` can not be null or blank");
    }

    @Nested
    class LargerGraphTest extends RandomGraphTestCase {

        @Override
        @BeforeEach
        protected void setupGraph() {
            buildGraph(10000);
        }

        @Test
        void testLargerGraphSize() {
            runQuery(
                "MATCH (s:Label) " +
                    "OPTIONAL MATCH (s)-[r:TYPE]->(t:Label) " +
                    "RETURN gds.graph.project('g', s, t)"
            );
            assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
            var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();
            assertThat(graph.nodeCount()).isEqualTo(10000);
        }
    }

    @Nested
    class InverseGraphTest extends BaseTest {

        long nodeCount;
        long relCount;

        @BeforeEach
        protected void setupGraph() {
            runQuery(
                "CREATE (a:A)" +
                    ",(a)-[:REL {weight:rand()}]->(:B) " +
                    ",(a)-[:REL {weight:rand()}]->(:B)"
            );
            runQuery("FOREACH (x IN range(1, 1000) | CREATE (:A))");

            nodeCount = runQuery(
                "MATCH (n) RETURN COUNT(n) AS count",
                r -> r.<Long>columnAs("count").stream().findFirst().get()
            );
            relCount = runQuery(
                "MATCH ()-->() RETURN COUNT(*) AS count",
                r -> r.<Long>columnAs("count").stream().findFirst().get()
            );
        }

        @Test
        void testInverseIds() {
            runQuery(
                "MATCH (s:A) " +
                    "OPTIONAL MATCH (s)-[r:REL]->(t:B) " +
                    "WITH s, t, r " +
                    "ORDER BY id(s) DESC " +
                    "RETURN gds.graph.project('g', s, t)"
            );

            assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
            var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();
            assertThat(graph.nodeCount()).isEqualTo(nodeCount);
            assertThat(graph.relationshipCount()).isEqualTo(relCount);
        }

        @Test
        void testInverseIdsWithProperties() {
            runQuery(
                "MATCH (s:A) " +
                    "OPTIONAL MATCH (s)-[r:REL]->(t:B) " +
                    "WITH s, t, r " +
                    "ORDER BY id(s) DESC " +
                    "RETURN gds.graph.project('g', s, t," +
                    " { relationshipProperties: { weight: coalesce(r.weight, 13.37) } }" +
                    ")"
            );

            assertThat(GraphStoreCatalog.exists("", db.databaseName(), "g")).isTrue();
            var graph = GraphStoreCatalog.get("", db.databaseName(), "g").graphStore().getUnion();
            assertThat(graph.nodeCount()).isEqualTo(nodeCount);
            assertThat(graph.relationshipCount()).isEqualTo(relCount);
        }
    }
}
