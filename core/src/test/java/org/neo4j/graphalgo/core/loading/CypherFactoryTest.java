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
package org.neo4j.graphalgo.core.loading;

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.block.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.CypherLoaderBuilder;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.TestGraphLoader;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.TestSupport.FactoryType.CYPHER;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.applyInTransaction;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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

        String nodes = "MATCH (n) RETURN id(n) AS id, COALESCE(n.partition, 0) AS partition , COALESCE(n.foo, 5.0) AS foo";
        String rels = "MATCH (n)-[r]->(m) WHERE type(r) = 'REL' " +
                      "RETURN id(n) AS source, id(m) AS target, coalesce(head(collect(r.prop)), 0)";

        Graph graph = applyInTransaction(db, tx -> new CypherLoaderBuilder().api(db)
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
                    .api(db)
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

        Graph graph = TestGraphLoader
            .from(db)
            .withNodeProperties(PropertyMappings.of(prop1, prop2, prop3))
            .graph(CYPHER);

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

        GraphStore graphs = TestGraphLoader
            .from(db)
            .withRelationshipProperties(PropertyMappings.of(prop1, prop2, prop3), false)
            .withDefaultAggregation(Aggregation.DEFAULT)
            .graphStore(CYPHER);

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
            .api(db)
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
            .api(db)
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
            Pair.of(Set.of(NodeLabel.of("A")), Set.of(NodeLabel.of("B"))),
            Pair.of(Set.of(NodeLabel.of("A")), Set.of(NodeLabel.of("A"), NodeLabel.of("B")))
        );

        var actual = new ArrayList<Pair<Set<NodeLabel>, Set<NodeLabel>>>();
        abGraph.forEachNode(nodeId -> {
            abGraph.forEachRelationship(nodeId, (source, target) -> {
                actual.add(Pair.of(abGraph.nodeLabels(source), abGraph.nodeLabels(target)));
                return true;
            });
            return true;
        });
        assertThat(actual).containsExactlyElementsOf(expectation);
    }

    @Test
    void testFailIfLabelColumnIsEmpty() {
        GraphLoader loader = new CypherLoaderBuilder()
            .api(db)
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
            .api(db)
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
    @Disabled("Why did we need this? https://github.com/neo-technology/graph-analytics/pull/1079")
    void failOnDuplicateNodeIds() {
        GraphLoader loader = new CypherLoaderBuilder()
            .api(db)
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

    @ParameterizedTest(name =  "{0}")
    @MethodSource("memoryEstimationVariants")
    void testMemoryEstimation(String description, String nodeQuery, String relQuery, long min, long max) {
        GraphLoader loader = new CypherLoaderBuilder()
            .api(db)
            .nodeQuery(nodeQuery)
            .relationshipQuery(relQuery)
            .build();

        CypherFactory factory = (CypherFactory) loader.graphStoreFactory();
        MemoryEstimation memoryEstimation = factory.memoryEstimation();
        MemoryTree estimate = memoryEstimation.estimate(factory.estimationDimensions(), 4);

        assertEquals(min ,estimate.memoryUsage().min);
        assertEquals(max ,estimate.memoryUsage().max);
    }

    private static Stream<Arguments> memoryEstimationVariants() {
        return Stream.of(
            Arguments.of(
                "Topology Only",
                "MATCH (n) RETURN id(n) as id",
                "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target",
                520840,
                520840
            ),

            Arguments.of(
                "Node properties",
                "MATCH (n) RETURN id(n) as id, n.id as idProp",
                "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target",
                619296,
                619296
            ),

            Arguments.of(
                "Relationship properties",
                "MATCH (n) RETURN id(n) as id",
                "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, r.prop as prop",
                863096,
                863096
            )
        );
    }

    private void loadAndTestGraph(
        String nodeStatement,
        String relStatement
    ) {
        CypherLoaderBuilder builder = new CypherLoaderBuilder()
            .api(db)
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
