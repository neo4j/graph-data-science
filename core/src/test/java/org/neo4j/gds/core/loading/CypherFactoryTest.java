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
package org.neo4j.gds.core.loading;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.block.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.CypherLoaderBuilder;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.TestGraphLoaderFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.MapUtil;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.GraphFactoryTestSupport.FactoryType.CYPHER;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.applyInTransaction;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class CypherFactoryTest extends BaseTest {

    private static final int COUNT = 10_000;
    private static final String DB_CYPHER = "UNWIND range(1, $count) AS id " +
                                            "CREATE (n {id: id})-[:REL {prop: id % 10}]->(n)";

    @BeforeEach
    void setUp() {
        runQuery(DB_CYPHER, MapUtil.map("count", COUNT));
    }

    @Test
    void testLoadCypher() {
        clearDb();
        String query = " CREATE (n1 {partition: 6})-[:REL {prop: 1}]->(n2 {foo: 4.0})-[:REL {prop: 2}]->(n3)" +
                       " CREATE (n1)-[:REL {prop: 3}]->(n3)" +
                       " RETURN id(n1) AS id1, id(n2) AS id2, id(n3) AS id3";
        runQuery(query);

        String nodes = "MATCH (n) RETURN id(n) AS id, COALESCE(n.partition, 0) AS partition, COALESCE(n.foo, 5.0) AS foo";
        String rels = "MATCH (n)-[r]->(m) WHERE type(r) = 'REL' " +
                      "RETURN id(n) AS source, id(m) AS target, coalesce(head(collect(r.prop)), 0)";

        Graph graph = applyInTransaction(db, tx -> new CypherLoaderBuilder().databaseService(db)
                .nodeQuery(nodes)
                .relationshipQuery(rels)
                .build()
                .graph()
        );

        assertGraphEquals(fromGdl(
            "(a {partition: 6, foo: 5.0})" +
            "(b {partition: 0, foo: 4.0})" +
            "(c {partition: 0, foo: 5.0})" +
            "(a)-[{w:1.0}]->(b)" +
            "(a)-[{w:3.0}]->(c)" +
            "(b)-[{w:2.0}]->(c)"
        ), graph);
    }

    @Test
    void testReadOnly() {
        String nodes = "MATCH (n) SET n.name='foo' RETURN id(n) AS id";
        String relationships = "MATCH (n)-[r]->(m) WHERE type(r) = 'REL' RETURN id(n) AS source, id(m) AS target, r.prop AS weight ORDER BY id(r) DESC ";

        IllegalArgumentException readOnlyException = assertThrows(
            IllegalArgumentException.class,
            () -> {
                GraphLoader build = new CypherLoaderBuilder()
                    .databaseService(db)
                    .nodeQuery(nodes)
                    .relationshipQuery(relationships)
                    .build();
                build
                    .graph();
            }
        );

        assertTrue(readOnlyException.getMessage().contains("Query must be read only"));
    }

    @Test
    void testLoadRelationshipsCypher() {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String relStatement = "MATCH (n)-[r:REL]->(m) RETURN id(n) AS source, id(m) AS target, r.prop AS weight";

        loadAndTestGraph(nodeStatement, relStatement);
    }

    @Test
    void testLoadZeroRelationshipsCypher() {
        String nodeStatement = "MATCH (n) RETURN id(n) AS id";
        String relStatement = "MATCH (n)-[r:DOES_NOT_EXIST]->(m) RETURN id(n) AS source, id(m) AS target";

        CypherLoaderBuilder builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeStatement)
            .relationshipQuery(relStatement);

        Graph graph = applyInTransaction(db, tx -> builder.build().graph());

        assertThat(graph.nodeCount()).isEqualTo(COUNT);
        assertThat(graph.relationshipCount()).isEqualTo(0);
    }

    @Test
    void doubleListNodeProperty() {
        var nodeQuery = "RETURN 0 AS id, [1.3, 3.7] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        var graph = applyInTransaction(db, tx -> builder.build().graph());
        assertThat(graph.nodeProperties("list").doubleArrayValue(0)).containsExactly(1.3, 3.7);
    }

    @Test
    void doubleListWithEmptyList() {
        var nodeQuery = "WITH [0, 1] AS ids, [[1.3, 3.7], []] AS properties " +
                        "UNWIND ids AS id " +
                        "RETURN id, properties[id] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        var graph = applyInTransaction(db, tx -> builder.build().graph());
        assertThat(graph.nodeProperties("list").doubleArrayValue(0)).containsExactly(1.3, 3.7);
        assertThat(graph.nodeProperties("list").doubleArrayValue(1)).isEmpty();
    }

    @Test
    void longListWithEmptyList() {
        var nodeQuery = "WITH [0, 1] AS ids, [[1, 3, 3, 7], []] AS properties " +
                        "UNWIND ids AS id " +
                        "RETURN id, properties[id] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        var graph = applyInTransaction(db, tx -> builder.build().graph());
        assertThat(graph.nodeProperties("list").longArrayValue(0)).containsExactly(1, 3, 3, 7);
        assertThat(graph.nodeProperties("list").longArrayValue(1)).isEmpty();
    }

    @Test
    void longListNodeProperty() {
        var nodeQuery = "RETURN 0 AS id, [1, 2] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        var graph = applyInTransaction(db, tx -> builder.build().graph());
        assertThat(graph.nodeProperties("list").longArrayValue(0)).containsExactly(1L, 2L);
    }

    @Test
    void emptyList() {
        var nodeQuery = "RETURN 0 AS id, [] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        var graph = applyInTransaction(db, tx -> builder.build().graph());
        assertThat(graph.nodeProperties("list").longArrayValue(0)).isEmpty();
    }

    @Test
    void failsOnBadMixedList() {
        var nodeQuery = "RETURN 0 AS id, [1, 2, true] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        assertThatThrownBy(() -> applyInTransaction(db, tx -> builder.build().graph()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Only lists of uniformly typed numbers are supported as GDS node properties")
            .hasMessageContaining("List{Long(1), Long(2), Boolean('true')}");
    }

    @Test
    void failsOnMixedNumbersList() {
        var nodeQuery = "RETURN 0 AS id, [1, 2, 1.23] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        assertThatThrownBy(() -> applyInTransaction(db, tx -> builder.build().graph()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Only lists of uniformly typed numbers are supported as GDS node properties")
            .hasMessageContaining("List{Long(1), Long(2), Double(1"); // omitting the rest of the double for locale reasons
    }

    @Test
    void failsOnBadUniformList() {
        var nodeQuery = "RETURN 0 AS id, ['forty', 'two'] AS list";

        var builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery("RETURN 0 AS source, 0 AS target LIMIT 0");

        assertThatThrownBy(() -> applyInTransaction(db, tx -> builder.build().graph()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Only lists of uniformly typed numbers are supported as GDS node properties")
            .hasMessageContaining("List{String(\"forty\"), String(\"two\")}");
    }

    @Test
    void testMultipleNodeProperties() {
        clearDb();
        runQuery(
            "CREATE" +
            "  ({prop1: 1})" +
            ", ({prop2: 2})" +
            ", ({prop3: 3})"
        );
        PropertyMapping prop1 = PropertyMapping.of("prop1", 0);
        PropertyMapping prop2 = PropertyMapping.of("prop2", 0);
        PropertyMapping prop3 = PropertyMapping.of("prop3", 0);

        Graph graph = TestGraphLoaderFactory.graphLoader(db, CYPHER)
            .withNodeProperties(PropertyMappings.of(prop1, prop2, prop3))
            .graph();

        String gdl = "(a {prop1: 1, prop2: 0, prop3: 0})" +
                     "(b {prop1: 0, prop2: 2, prop3: 0})" +
                     "(c {prop1: 0, prop2: 0, prop3: 3})";

        assertGraphEquals(fromGdl(gdl), graph);
    }

    @Test
    void testMultipleRelationshipProperties() {
        clearDb();
        runQuery(
            "CREATE" +
            "  (n1)" +
            ", (n2)" +
            ", (n1)-[:REL {prop1: 1.0}]->(n2)" +
            ", (n1)-[:REL {prop2: 2.0}]->(n2)" +
            ", (n1)-[:REL {prop3: 3.0}]->(n2)"
        );
        PropertyMapping prop1 = PropertyMapping.of("prop1", 0D);
        PropertyMapping prop2 = PropertyMapping.of("prop2", 0D);
        PropertyMapping prop3 = PropertyMapping.of("prop3", 42D);

        GraphStore graphs = TestGraphLoaderFactory.graphLoader(db, CYPHER)
            .withRelationshipProperties(PropertyMappings.of(prop1, prop2, prop3), false)
            .withDefaultAggregation(Aggregation.DEFAULT)
            .graphStore();

        String expectedGraph =
            "  (a)-[{w: %f}]->(b)" +
            ", (a)-[{w: %f}]->(b)" +
            ", (a)-[{w: %f}]->(b)";

        assertGraphEquals(
            fromGdl(formatWithLocale(expectedGraph, 1.0f, prop1.defaultValue().doubleValue(), prop1.defaultValue().doubleValue())),
            graphs.getGraph(ALL_RELATIONSHIPS, Optional.of(prop1.propertyKey()))
        );

        assertGraphEquals(
            fromGdl(formatWithLocale(expectedGraph, prop2.defaultValue().doubleValue(), 2.0, prop2.defaultValue().doubleValue())),
            graphs.getGraph(ALL_RELATIONSHIPS, Optional.of(prop2.propertyKey()))
        );

        assertGraphEquals(
            fromGdl(formatWithLocale(expectedGraph, prop3.defaultValue().doubleValue(), prop3.defaultValue().doubleValue(), 3.0)),
            graphs.getGraph(ALL_RELATIONSHIPS, Optional.of(prop3.propertyKey()))
        );
    }

    @Test
    void loadGraphWithParameterizedCypherQuery() {
        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery("MATCH (n) WHERE n.id = $nodeProp RETURN id(n) AS id, n.id as nodeProp")
            .relationshipQuery("MATCH (n)-[]->(m) WHERE n.id = $nodeProp and m.id = $nodeProp RETURN id(n) AS source, id(m) AS target, $relProp as relProp")
            .parameters(MapUtil.map("nodeProp", 42, "relProp", 21))
            .build();

        Graph graph = applyInTransaction(db, tx -> loader.graph());

        assertGraphEquals(fromGdl("(a { nodeProp: 42 })-[{ w: 21 }]->(a)"), graph);
    }

    @Test
    void testLoadingGraphWithLabelInformation() {
        clearDb();
        String query = "CREATE" +
                       "  (a:A)" +
                       ", (b:B)" +
                       ", (c:C)" +
                       ", (ab:A:B)" +
                       "CREATE" +
                       "  (a)-[:REL]->(b)" +
                       ", (a)-[:REL]->(c)" +
                       ", (a)-[:REL]->(ab)" +
                       ", (c)-[:REL]->(a)";


        runQuery(query);

        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery("MATCH (n) RETURN id(n) AS id, labels(n) as labels")
            .relationshipQuery("MATCH (n)-[]->(m) RETURN id(n) AS source, id(m) AS target")
            .validateRelationships(false)
            .build();

        GraphStore graphStore = applyInTransaction(db, tx -> loader.graphStore());

        Function<List<String>, Graph> getGraph = (List<String> labels) -> graphStore.getGraph(
            labels.stream().map(NodeLabel::of).collect(Collectors.toList()),
            Collections.singletonList(ALL_RELATIONSHIPS),
            Optional.empty()
        );

        assertEquals(4, graphStore.nodeCount());
        assertEquals(2, getGraph.apply(Collections.singletonList("A")).nodeCount());
        assertEquals(2, getGraph.apply(Collections.singletonList("B")).nodeCount());
        var abGraph = getGraph.apply(List.of("A", "B"));
        assertEquals(3, abGraph.nodeCount());

        var expectation = List.of(
            Pair.of(List.of(NodeLabel.of("A")), List.of(NodeLabel.of("B"))),
            Pair.of(List.of(NodeLabel.of("A")), List.of(NodeLabel.of("A"), NodeLabel.of("B")))
        );

        var actual = new ArrayList<Pair<List<NodeLabel>, List<NodeLabel>>>();
        abGraph.forEachNode(nodeId -> {
            abGraph.forEachRelationship(nodeId, (source, target) -> {
                actual.add(Pair.of(abGraph.nodeLabels(source), abGraph.nodeLabels(target)));
                return true;
            });
            return true;
        });

        assertThat(actual).containsExactlyInAnyOrderElementsOf(expectation);
    }

    @Test
    void testFailIfLabelColumnIsEmpty() {
        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery("RETURN 1 AS id, [] as labels")
            .relationshipQuery("MATCH (n)-[]->(m) RETURN id(n) AS source, id(m) AS target")
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> applyInTransaction(db, tx -> loader.graphStore())
        );

        assertTrue(ex.getMessage().contains("does not specify a label"));
    }

    @Test
    void testFailIfLabelColumnIsOfWrongType() {
        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery("RETURN 1 AS id, 42 as labels")
            .relationshipQuery("MATCH (n)-[]->(m) RETURN id(n) AS source, id(m) AS target")
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> applyInTransaction(db, tx -> loader.graphStore())
        );

        assertTrue(ex.getMessage().contains("should be of type List"));
    }

    @Test
    void canLoadArrayNodeProperties() {
        clearDb();
        String query = "Create (:A {longArray: [42], doubleArray: [42.0]})";
        runQuery(query);

        String nodes = "MATCH (n) RETURN id(n) AS id, n.longArray as longArray, n.doubleArray as doubleArray";
        String rels = "MATCH (n)" +
                      "RETURN id(n) AS source, id(n) AS target";

        Graph graph = applyInTransaction(db, tx -> new CypherLoaderBuilder().databaseService(db)
            .nodeQuery(nodes)
            .relationshipQuery(rels)
            .build()
            .graph()
        );

        assertGraphEquals(fromGdl("(a {longArray: [42L], doubleArray: [42.0D]}), (a)-->(a)"), graph);
    }

    @Test
    @Disabled("Why did we need this? https://github.com/neo-technology/graph-analytics/pull/1079")
    void failOnDuplicateNodeIds() {
        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery("UNWIND [42, 42] AS id RETURN id")
            .relationshipQuery("UNWIND [42, 42] AS id RETURN id AS source, id AS target")
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> applyInTransaction(db, tx -> loader.graphStore())
        );

        assertEquals(
            "Node(42) was added multiple times. Please make sure that the nodeQuery returns distinct ids.",
            ex.getMessage()
        );
    }

    @Test
    void shouldFailImmediatelyDuringNodeLoading() {
        // Inserting this many nodes will lead to at least one flush operation
        // on the NodesBatchBuffer. If an exception occurred before flushing,
        // i.e. due to a value conversion, the process should stop immediately,
        // and we should never run into an AIOOB in the buffers.
        var nodeCount = 2 * ParallelUtil.DEFAULT_BATCH_SIZE;

        clearDb();
        // create a node without `prop`
        runQuery("CREATE (n {id: 0})");
        // create more nodes with `prop`
        runQuery("UNWIND range(1, $count) AS id CREATE (n {id: id, prop: 0.1337})", Map.of("count", nodeCount));

        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            // The `42` in coalesce will signal to the NodesBuilder that the type is `long`.
            // However, all subsequent values will be floats, a conversion will fail for the
            // second record, and we should see this immediately during construction.
            .nodeQuery("MATCH (n) RETURN id(n) AS id, COALESCE(n.prop, 42) AS prop ORDER BY n.id ASC")
            .relationshipQuery("MATCH (n)-[]->(m) RETURN id(n) AS source, id(m) AS target")
            .build();

        var ex = assertThrows(
            UnsupportedOperationException.class,
            loader::graphStore
        );

        assertThat(ex).hasMessageContaining("Cannot safely convert");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("memoryEstimationVariants")
    void testMemoryEstimation(String description, String nodeQuery, String relQuery, long min, long max) {
        GraphLoader loader = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery(relQuery)
            .build();

        CypherFactory factory = (CypherFactory) loader.graphStoreFactory();
        MemoryEstimation memoryEstimation = factory.estimateMemoryUsageDuringLoading();
        MemoryTree estimate = memoryEstimation.estimate(factory.estimationDimensions(), 4);

        assertEquals(min, estimate.memoryUsage().min);
        assertEquals(max, estimate.memoryUsage().max);
    }

    private static Stream<Arguments> memoryEstimationVariants() {
        return Stream.of(
            Arguments.of(
                "Topology Only",
                "MATCH (n) RETURN id(n) as id",
                "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target",
                1202360,
                1202360
            ),

            Arguments.of(
                "Node properties",
                "MATCH (n) RETURN id(n) as id, n.id as idProp",
                "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target",
                1300800,
                1300800
            ),

            Arguments.of(
                "Relationship properties",
                "MATCH (n) RETURN id(n) as id",
                "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, r.prop as prop",
                1692720,
                1692720
            )
        );
    }

    private void loadAndTestGraph(
        String nodeStatement,
        String relStatement
    ) {
        CypherLoaderBuilder builder = new CypherLoaderBuilder()
            .databaseService(db)
            .nodeQuery(nodeStatement)
            .relationshipQuery(relStatement);

        Graph graph = applyInTransaction(db, tx -> builder.build().graph());

        assertEquals(COUNT, graph.nodeCount());
        AtomicInteger relCount = new AtomicInteger();
        graph.forEachNode(node -> {
            relCount.addAndGet(graph.degree(node));
            return true;
        });
        assertEquals(COUNT, relCount.get());
        AtomicInteger total = new AtomicInteger();
        graph.forEachNode(n -> {
            graph.forEachRelationship(n, Double.NaN, (s, t, w) -> {
                total.addAndGet((int) w);
                return true;
            });
            return true;
        });
        assertEquals(9 * COUNT / 2, total.get());
    }
}
