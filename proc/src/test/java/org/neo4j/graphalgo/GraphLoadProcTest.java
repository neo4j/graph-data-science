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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.labelpropagation.LabelPropagationWriteProc;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphalgo.pagerank.PageRankWriteProc;
import org.neo4j.graphalgo.wcc.WccWriteProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.GraphHelper.assertOutProperties;
import static org.neo4j.graphalgo.GraphHelper.assertOutPropertiesWithDelta;
import static org.neo4j.graphalgo.GraphHelper.assertOutRelationships;
import static org.neo4j.graphalgo.TestSupport.allGraphNamesAndDirections;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.toArguments;

/**
 * TODO:
 * Tests in this class are currently disabled as the legacy catalog procedures have been removed.
 * We can't remove the tests yet, as we might want to harvest them for useful tests to port.
 */
class GraphLoadProcTest extends BaseProcTest {

    private static final String ALL_NODES_QUERY = "'MATCH (n) RETURN id(n) AS id'";
    private static final String ALL_RELATIONSHIPS_QUERY = "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'";
    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:A {id: 0, partition: 42})" +
            ", (b:B {id: 1, partition: 42})" +

            ", (a)-[:X { weight: 1.0 }]->(:A {id: 2,  weight: 1.0, partition: 1})" +
            ", (a)-[:X { weight: 1.0 }]->(:A {id: 3,  weight: 2.0, partition: 1})" +
            ", (a)-[:X { weight: 1.0 }]->(:A {id: 4,  weight: 1.0, partition: 1})" +
            ", (a)-[:Y { weight: 1.0 }]->(:A {id: 5,  weight: 1.0, partition: 1})" +
            ", (a)-[:Z { weight: 1.0 }]->(:A {id: 6,  weight: 8.0, partition: 2})" +

            ", (b)-[:X { weight: 42.0 }]->(:B {id: 7,  weight: 1.0, partition: 1})" +
            ", (b)-[:X { weight: 42.0 }]->(:B {id: 8,  weight: 2.0, partition: 1})" +
            ", (b)-[:X { weight: 42.0 }]->(:B {id: 9,  weight: 1.0, partition: 1})" +
            ", (b)-[:Y { weight: 42.0 }]->(:B {id: 10, weight: 1.0, partition: 1})" +
            ", (b)-[:Z { weight: 42.0 }]->(:B {id: 11, weight: 8.0, partition: 2})";

    static Stream<Arguments> graphTypesFullGraph() {
        return Stream.of(
            Arguments.of("huge", null, null),
            Arguments.of("cypher", "MATCH (n) RETURN id(n) AS id", "MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target")
        );
    }

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(
            GraphCreateProc.class,
            LabelPropagationWriteProc.class,
            PageRankWriteProc.class,
            WccWriteProc.class
        );
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        GraphCatalog.removeAllLoadedGraphs();
    }

    @Disabled
    @ParameterizedTest
    @MethodSource("graphTypesFullGraph")
    void shouldLoadGraph(String graphType, String nodeQuery, String relationshipQuery) {
        String query = "CALL algo.graph.load(" +
                       "    'foo', $nodeQuery, $relationshipQuery, {" +
                       "        graph: $graphType" +
                       "    }" +
                       ")";

        runQueryWithRowConsumer(query, MapUtil.map("graphType", graphType, "nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery),
            row -> {
                assertEquals(12, row.getNumber("nodes").intValue());
                assertEquals(10, row.getNumber("relationships").intValue());
                assertEquals(graphType, row.getString("graph"));
                assertFalse(row.getBoolean("alreadyLoaded"));
            }
        );
    }

    static Stream<Arguments> relationshipWeightParameters() {
        return TestSupport.crossArguments(
            () -> Stream.of(
                Arguments.of("huge", null, null),
                Arguments.of(
                    "cypher",
                    "MATCH (n) RETURN id(n) AS id",
                    "MATCH (s)-[r]->(t) RETURN id(s) AS source, id(t) AS target, r.weight AS weight"
                )
            ),
            toArguments(() -> Stream.of("relationshipWeight", "weightProperty"))
        );
    }

    @Disabled
    @ParameterizedTest(name = "graphType = {0}, relationshipWeightParameter = {3}")
    @MethodSource("relationshipWeightParameters")
    void shouldLoadGraphWithRelationshipWeight(String graphType, String nodeQuery, String relationshipQuery, String relationshipWeightParam) {
        String query = "CALL algo.graph.load(" +
                       "    'foo', $nodeQuery, $relationshipQuery, {" +
                       "        graph: $graphType, " + relationshipWeightParam + ": 'weight'" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("graphType", graphType, "nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery));

        Graph fooGraph = GraphCatalog.getUnion(getUsername(), "foo").orElse(null);
        assertNotNull(fooGraph);
        assertEquals(12, fooGraph.nodeCount());
        assertEquals(10, fooGraph.relationshipCount());

        assertOutProperties(fooGraph, 0, 1.0, 1.0, 1.0, 1.0, 1.0);
        assertOutProperties(fooGraph, 1, 42.0, 42.0, 42.0, 42.0, 42.0);
    }

    @Disabled
    @ParameterizedTest
    @MethodSource("graphTypesFullGraph")
    void shouldLoadGraphWithSaturatedThreadPool(String graphType, String nodeQuery, String relationshipQuery) {
        // ensure that we don't drop task that can't be scheduled while importing a graph.

        String query = "CALL algo.graph.load('foo', $nodeQuery, $relationshipQuery, {graph: $graphType, batchSize: 2})";

        List<Future<?>> futures = new ArrayList<>();
        // block all available threads
        for (int i = 0; i < Pools.DEFAULT_CONCURRENCY; i++) {
            futures.add(
                    Pools.DEFAULT.submit(() -> LockSupport.parkNanos(Duration.ofSeconds(1).toNanos()))
            );
        }

        try {
            runQueryWithRowConsumer(query, MapUtil.map("graphType", graphType, "nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery),
                    row -> {
                        assertEquals(12, row.getNumber("nodes").intValue());
                        assertEquals(10, row.getNumber("relationships").intValue());
                        assertEquals(graphType, row.getString("graph"));
                        assertFalse(row.getBoolean("alreadyLoaded"));
                    }
            );
        } finally {
            ParallelUtil.awaitTermination(futures);
        }
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldLoadGraphWithMultipleNodeProperties(String graphType) throws KernelException {
        GraphDatabaseAPI testLocalDb = TestDatabaseCreator.createTestDatabase();
        testLocalDb.getDependencyResolver().resolveDependency(Procedures.class);

        String testGraph =
                "CREATE" +
                "  (a: Node { foo: 42, bar: 13.37 })" +
                ", (b: Node { foo: 43, bar: 13.38 })" +
                ", (c: Node { foo: 44, bar: 13.39 })" +
                ", (d: Node { foo: 45 })";

        runQuery(testLocalDb, testGraph, Collections.emptyMap());

        String loadQueryTemplate = "CALL algo.graph.load(" +
                           "    'fooGraph', '%s', '%s', {" +
                           "        graph: '%s'," +
                           "        nodeProperties: {" +
                           "            fooProp: 'foo'," +
                           "            barProp: {" +
                           "                property: 'bar', " +
                           "                defaultValue: 19.84" +
                           "            }" +
                           "        }" +
                           "    }" +
                           ")";

        String loadQuery;
        if (graphType.equalsIgnoreCase("cypher")) {
            loadQuery = String.format(loadQueryTemplate,
                "MATCH (n:Node) RETURN id(n) AS id, n.foo AS foo, n.bar AS bar",
                "MATCH (n:Node)-->(m:Node) RETURN id(n) AS sourceId, id(m) AS targetId",
                graphType);
        } else {
            loadQuery = String.format(loadQueryTemplate, "Node", "", graphType);
        }

        runQueryWithRowConsumer(testLocalDb, loadQuery, row -> {
            Map<String, Object> nodeProperties = (Map<String, Object>) row.get("nodeProperties");
            assertEquals(2, nodeProperties.size());

            String fooProp = nodeProperties.get("fooProp").toString();
            Map<String, Object> barPropParams = (Map<String, Object>) nodeProperties.get("barProp");

            assertEquals("foo", fooProp);
            assertEquals("bar", barPropParams.get("property").toString());
            assertEquals(19.84, (Double) barPropParams.get("defaultValue"));

        });

        Graph loadedGraph = GraphCatalog.getUnion(getUsername(), "fooGraph").orElse(null);
        Graph expected = TestGraph.Builder.fromGdl("({ fooProp: 42, barProp: 13.37D })" +
                                                   "({ fooProp: 43, barProp: 13.38D })" +
                                                   "({ fooProp: 44, barProp: 13.39D })" +
                                                   "({ fooProp: 45, barProp: 19.84D })");
        assertNotNull(loadedGraph);
        assertGraphEquals(expected, loadedGraph);

        GraphCatalog.remove(getUsername(), "fooGraph");
        testLocalDb.shutdown();
    }

    static Stream<Arguments> graphTypesWithRelationshipFilter() {
        return Stream.of(
            Arguments.of("huge", "", "X | Y"),
            Arguments.of("cypher", "MATCH (n) RETURN id(n) AS id", "MATCH (s)-[r:X|Y]->(t) RETURN type(r) AS type, id(s) AS source, id(t) AS target")
        );
    }

    @Disabled
    @ParameterizedTest(name = "graphType = {0}, relTypeQuery = {1}")
    @MethodSource("graphTypesWithRelationshipFilter")
    void shouldLoadGraphWithMultipleRelationships(String graphType, String nodeQuery, String relationshipQuery) {
        String query = "CALL algo.graph.load(" +
                       "    'foo', $nodeQuery, $relationshipQuery, {" +
                       "        graph: $graphType" +
                       "    }" +
                       ")";

        runQueryWithRowConsumer(
            query,
            MapUtil.map("nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery, "graphType", graphType),
            row -> {
                assertEquals(12, row.getNumber("nodes").intValue());
                assertEquals(8, row.getNumber("relationships").intValue());
                assertEquals(graphType, row.getString("graph"));
                assertFalse(row.getBoolean("alreadyLoaded"));
            }
        );
    }

    @Disabled
    @AllGraphNamesTest
    void shouldFailToLoadGraphWithMultipleRelationships(String graphImpl) {
        String query = String.format("CALL algo.graph.load(" +
                                     "    'foo', 'null', 'X | Y', {" +
                                     "        graph: '%s'" +
                                     "    }" +
                                     ")", graphImpl);

        assertThrows(QueryExecutionException.class, () -> runQuery(query));
    }

    @Disabled
    @Test
    void shouldLoadGraphWithMultipleRelationshipProperties() throws KernelException {
        GraphDatabaseAPI testLocalDb = TestDatabaseCreator.createTestDatabase();
        testLocalDb.getDependencyResolver().resolveDependency(Procedures.class);

        String testGraph =
                "CREATE" +
                "  (a: Node)" +
                ", (b: Node)" +
                ", (a)-[:TYPE_1 { weight: 42.1, cost: 1 }]->(b)" +
                ", (a)-[:TYPE_1 { weight: 43.2, cost: 2 }]->(b)" +
                ", (a)-[:TYPE_2 { weight: 44.3, cost: 3 }]->(b)" +
                ", (a)-[:TYPE_2 { weight: 45.4, cost: 4 }]->(b)";

        runQuery(testLocalDb, testGraph, Collections.emptyMap());

        String loadQuery = "CALL algo.graph.load(" +
                           "    'aggGraph', 'Node', 'TYPE_1', {" +
                           "        relationshipProperties: {" +
                           "            sumWeight: {" +
                           "                property: 'weight'," +
                           "                aggregation: 'SUM'," +
                           "                defaultValue: 1.0" +
                           "            }," +
                           "            minWeight: {" +
                           "                property: 'weight'," +
                           "                aggregation: 'MIN'" +
                           "            }," +
                           "            maxCost: {" +
                           "                property: 'cost'," +
                           "                aggregation: 'MAX'" +
                           "            }" +
                           "        }" +
                           "    }" +
                           ")";

        runQueryWithRowConsumer(testLocalDb, loadQuery, row -> {
            Map<String, Object> relProperties = (Map<String, Object>) row.get("relationshipProperties");
            assertEquals(3, relProperties.size());

            Map<String, Object> sumWeightParams = (Map<String, Object>) relProperties.get("sumWeight");
            Map<String, Object> minWeightParams = (Map<String, Object>) relProperties.get("minWeight");
            Map<String, Object> maxCostParams = (Map<String, Object>) relProperties.get("maxCost");

            assertEquals("weight", sumWeightParams.get("property").toString());
            assertEquals("SUM", sumWeightParams.get("aggregation").toString());
            assertEquals(1.0, sumWeightParams.get("defaultValue"));

            assertEquals("weight", minWeightParams.get("property").toString());
            assertEquals("MIN", minWeightParams.get("aggregation").toString());

            assertEquals("cost", maxCostParams.get("property").toString());
            assertEquals("MAX", maxCostParams.get("aggregation").toString());
        });

        Graph g = GraphCatalog.getUnion(getUsername(), "aggGraph").orElse(null);

        assertNotNull(g);
        assertEquals(2, g.nodeCount());
        assertEquals(3, g.relationshipCount());

        assertOutRelationships(g, 0, 1, 1, 1);
        assertOutPropertiesWithDelta(g, 1E-3, 0, 85.3, 42.1, 2.0);

        GraphCatalog.remove(getUsername(), "aggGraph");
        testLocalDb.shutdown();
    }

    @Disabled
    @Test
    void shouldFailOnMissingRelationshipProperty() {
        QueryExecutionException exMissingProperty = assertThrows(QueryExecutionException.class, () -> {
            String loadQuery = "CALL algo.graph.load(" +
                               "    'aggGraph', '', '', {" +
                               "        relationshipProperties: {" +
                               "            maxCost: {" +
                               "                property: 'cost'," +
                               "                aggregation: 'MAX'" +
                               "            }" +
                               "        }" +
                               "    }" +
                               ")";
            runQuery(loadQuery);
        });
        Throwable rootCause = ExceptionUtil.rootCause(exMissingProperty);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Relationship properties not found: 'cost'"));
    }

    @Disabled
    @Test
    void shouldFailOnInvalidAggregationFunction() {
        QueryExecutionException exMissingProperty = assertThrows(QueryExecutionException.class, () -> {
            String loadQuery = "CALL algo.graph.load(" +
                               "    'aggGraph', '', '', {" +
                               "        relationshipProperties: {" +
                               "            maxCost: {" +
                               "                property: 'weight'," +
                               "                aggregation: 'FOOBAR'" +
                               "            }" +
                               "        }" +
                               "    }" +
                               ")";
            runQuery(loadQuery);
        });
        Throwable rootCause = ExceptionUtil.rootCause(exMissingProperty);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Deduplication strategy `FOOBAR` is not supported."));
    }

    @Disabled
    @Test
    void shouldComputeMemoryEstimationForHuge() {
        String query = "CALL algo.graph.load.memrec(" +
                       "    null, null, {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD bytesMin, bytesMax";
        runQueryWithRowConsumer(query, singletonMap("graph", "huge"),
                row -> {
                    assertEquals(303520, row.getNumber("bytesMin").longValue());
                    assertEquals(303520, row.getNumber("bytesMax").longValue());
                }
        );
    }

    @Disabled
    @Test
    void shouldComputeMemoryEstimationForHugeWithProperties() {
        String query = "CALL algo.graph.load.memrec(" +
                       "    null, null, {" +
                       "        graph: $graph, weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD bytesMin, bytesMax";

        runQueryWithRowConsumer(query, singletonMap("graph", "huge"),
                row -> {
                    assertEquals(573952, row.getNumber("bytesMin").longValue());
                    assertEquals(573952, row.getNumber("bytesMax").longValue());
                });
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldUseLoadedGraph(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSHIPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        runQuery(loadQuery, singletonMap("graph", graph));

        String algoQuery = "CALL gds.pageRank.write(" +
                           "    'foo', {" +
                           "        writeProperty: 'writingOnLoadedGraph'" +
                           "    }" +
                           ") YIELD nodePropertiesWritten";
        runQueryWithRowConsumer(algoQuery,
                row -> assertEquals(12, row.getNumber("nodePropertiesWritten").intValue()));
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void multiUseLoadedGraph(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSHIPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        runQuery(loadQuery, singletonMap("graph", graph));

        String algoQuery = "CALL gds.pageRank.write(" +
                           "    'foo', {" +
                           "        writeProperty: 'multiUseLoadedGraph'" +
                           "    }" +
                           ") YIELD nodePropertiesWritten";
        runQueryWithRowConsumer(algoQuery,
                row -> assertEquals(12, row.getNumber("nodePropertiesWritten").intValue()));
        runQueryWithRowConsumer(algoQuery,
                row -> assertEquals(12, row.getNumber("nodePropertiesWritten").intValue()));
    }

    @Disabled
    void multiUseLoadedGraphWithMultipleRelationships() {
        String query = "CALL gds.graph.create(" +
                       "    'foo', {}, 'X | Y'" +
                       ")";

        runQueryWithRowConsumer(
                query,
                row -> {
                    assertEquals(12, row.getNumber("nodes").intValue());
                    assertEquals(8, row.getNumber("relationships").intValue());
                    assertEquals("foo", row.getString("graphName"));
                }
        );

        String algoQuery = "CALL gds.wcc.stats(" +
                           "    'foo', {" +
                           "        relationshipProjection: $relType, writeProperty: 'componentId'" +
                           "    }" +
                           ")";
        runQueryWithRowConsumer(algoQuery, singletonMap("relType", Arrays.asList("X", "Y")),
                row -> assertEquals(4, row.getNumber("componentCount").intValue()));

        runQueryWithRowConsumer(algoQuery, singletonMap("relType", Arrays.asList("X")),
                row -> assertEquals(6, row.getNumber("componentCount").intValue()));

        runQueryWithRowConsumer(algoQuery, singletonMap("relType", Arrays.asList("Y")),
                row -> assertEquals(10, row.getNumber("componentCount").intValue()));
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldThrowIfGraphAlreadyLoaded(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ") YIELD alreadyLoaded AS loaded " +
                               "RETURN loaded";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSHIPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        Map<String, Object> params = singletonMap("graph", graph);
        // First load succeeds
        Boolean loaded = runQuery(query, params, result -> result.<Boolean>columnAs("loaded").next());
        assertFalse(loaded);
        // Second load throws exception
        QueryExecutionException exGraphAlreadyLoaded = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(query, params, Result::next)
        );
        Throwable rootCause = ExceptionUtil.rootCause(exGraphAlreadyLoaded);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), equalTo("A graph with name 'foo' is already loaded."));
    }


    static Stream<Arguments> graphImplAndDirectionAndComputeDegreeDistribution() {
        return Stream.of(true, false)
                .flatMap(computeDegreeDistribution -> allGraphNamesAndDirections()
                        .map(comb -> arguments(comb.get()[0], comb.get()[1], computeDegreeDistribution)));
    }

    @Disabled
    @ParameterizedTest(name = "graph = {0}, direction = {1}, computeDegreeDistribution = {2}")
    @MethodSource("graphImplAndDirectionAndComputeDegreeDistribution")
    void shouldReturnGraphInfoWithDegreeDistributionAndLoadDirection(
            String graphImpl,
            String loadDirection,
            Boolean computeDegreeDistribution) {
        String query = "CALL algo.graph.load(" +
                       "    'foo', null, null, {" +
                       "        graph: $graph, direction: $direction" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("graph", graphImpl, "direction", loadDirection));

        runQueryWithRowConsumer(
                "CALL algo.graph.info('foo', $computeDegreeDistr)",
                Collections.singletonMap("computeDegreeDistr", computeDegreeDistribution),
                resultRow -> {
                    assertEquals(graphImpl.toLowerCase(), resultRow.getString("type"));
                    assertEquals(loadDirection, resultRow.getString("direction"));
                });
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void removeGraph(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSHIPS_QUERY)
                : String.format(queryTemplate, "null", "null");
        runQuery(query, singletonMap("graph", graph));

        runQueryWithRowConsumer("CALL algo.graph.info($name, true)", singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph.equals("cypher") ? "huge" : graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("exists"));
        });
        runQueryWithRowConsumer("CALL algo.graph.remove($name)", singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph.equals("cypher") ? "huge" : graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("removed"));
        });
        runQueryWithRowConsumer("CALL algo.graph.info($name)", singletonMap("name", "foo"), row -> {
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @Disabled
    @Test
    void removeGraphWithMultipleRelationshipTypes() {
        String query = "CALL algo.graph.load(" +
                       "    'foo', null, 'X | Y', {" +
                       "        graph: 'huge'" +
                       "    }" +
                       ")";
        runQuery(query);

        runQueryWithRowConsumer("CALL algo.graph.info($name, true)", singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(8, row.getNumber("relationships").intValue());
            assertEquals("huge", row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("exists"));
        });
        runQueryWithRowConsumer("CALL algo.graph.remove($name)", singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(8, row.getNumber("relationships").intValue());
            assertEquals("huge", row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("removed"));
        });
        runQueryWithRowConsumer("CALL algo.graph.info($name)", singletonMap("name", "foo"), row -> {
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @Disabled
    @Test
    void removeMissingGraphIsNoOp() {
        runQueryWithRowConsumer("CALL algo.graph.remove($name)", singletonMap("name", "foo"), row -> {
            assertEquals(0, row.getNumber("nodes").intValue());
            assertEquals(0, row.getNumber("relationships").intValue());
            assertNull(row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("removed"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void degreeDistribution(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSHIPS_QUERY)
                : String.format(queryTemplate, "null", "null");
        runQuery(query, singletonMap("graph", graph));

        runQueryWithRowConsumer("CALL algo.graph.info($name, true)", singletonMap("name", "foo"), row -> {
            assertEquals(5, row.getNumber("max").intValue());
            assertEquals(0, row.getNumber("min").intValue());
            assertEquals(0.8333333, row.getNumber("mean").doubleValue(), 1e-4);
            assertEquals(0, row.getNumber("p50").intValue());
            assertEquals(0, row.getNumber("p75").intValue());
            assertEquals(5, row.getNumber("p90").intValue());
            assertEquals(5, row.getNumber("p95").intValue());
            assertEquals(5, row.getNumber("p99").intValue());
            assertEquals(5, row.getNumber("p999").intValue());
        });

        runQueryWithRowConsumer("CALL algo.graph.info($name, false)", singletonMap("name", "foo"), row -> {
            assertEquals(0, row.getNumber("max").intValue());
            assertEquals(0, row.getNumber("min").intValue());
            assertEquals(0, row.getNumber("mean").intValue());
            assertEquals(0, row.getNumber("p50").intValue());
            assertEquals(0, row.getNumber("p75").intValue());
            assertEquals(0, row.getNumber("p90").intValue());
            assertEquals(0, row.getNumber("p95").intValue());
            assertEquals(0, row.getNumber("p99").intValue());
            assertEquals(0, row.getNumber("p999").intValue());
        });

        runQueryWithRowConsumer("CALL algo.graph.info($name, {})", singletonMap("name", "foo"), row -> {
            assertEquals(0, row.getNumber("max").intValue());
            assertEquals(0, row.getNumber("min").intValue());
            assertEquals(0, row.getNumber("mean").intValue());
            assertEquals(0, row.getNumber("p50").intValue());
            assertEquals(0, row.getNumber("p75").intValue());
            assertEquals(0, row.getNumber("p90").intValue());
            assertEquals(0, row.getNumber("p95").intValue());
            assertEquals(0, row.getNumber("p99").intValue());
            assertEquals(0, row.getNumber("p999").intValue());
        });

        runQueryWithRowConsumer("CALL algo.graph.info($name, null)", singletonMap("name", "foo"), row -> {
            assertEquals(0, row.getNumber("max").intValue());
            assertEquals(0, row.getNumber("min").intValue());
            assertEquals(0, row.getNumber("mean").intValue());
            assertEquals(0, row.getNumber("p50").intValue());
            assertEquals(0, row.getNumber("p75").intValue());
            assertEquals(0, row.getNumber("p90").intValue());
            assertEquals(0, row.getNumber("p95").intValue());
            assertEquals(0, row.getNumber("p99").intValue());
            assertEquals(0, row.getNumber("p999").intValue());
        });
    }

    @Disabled
    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void incomingDegreeDistribution(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph, direction: 'IN'" +
                               "    }" +
                               ")";
        String loadQuery = graph.equals("cypher")
                ? String.format(
                queryTemplate,
                ALL_NODES_QUERY,
                "'MATCH (s)<--(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");
        runQuery(loadQuery, singletonMap("graph", graph));

        String infoQuery = graph.equals("cypher")
                ? "CALL algo.graph.info($name, {direction:'OUT'})"
                : "CALL algo.graph.info($name, {direction:'IN'})";
        runQueryWithRowConsumer(infoQuery, singletonMap("name", "foo"), row -> {
            assertEquals(1, row.getNumber("max").intValue());
            assertEquals(0, row.getNumber("min").intValue());
            assertEquals(0.8333333, row.getNumber("mean").doubleValue(), 1e-4);
            assertEquals(1, row.getNumber("p50").intValue());
            assertEquals(1, row.getNumber("p75").intValue());
            assertEquals(1, row.getNumber("p90").intValue());
            assertEquals(1, row.getNumber("p95").intValue());
            assertEquals(1, row.getNumber("p99").intValue());
            assertEquals(1, row.getNumber("p999").intValue());
        });
    }

    @Disabled
    @Test
    void shouldReturnEmptyList() {
        assertEmptyResult("CALL algo.graph.list() YIELD name, nodes, relationships, type, direction");
    }

    @Disabled
    @Test
    void shouldListAllAvailableGraphs() {
        String loadQuery = "CALL algo.graph.load(" +
                           "    $name, null, null, {" +
                           "        graph: $type, direction: $direction" +
                           "    }" +
                           ")" +
                           "YIELD nodes, relationships";

        List<Map<String, Object>> parameters = Arrays.asList(
                MapUtil.map("name", "foo", "type", "huge", "direction", "OUTGOING")
        );

        parameters.forEach((parameter) -> runQueryWithRowConsumer(loadQuery, parameter, resultRow -> {
                    parameter.put("nodes", resultRow.getNumber("nodes"));
                    parameter.put("relationships", resultRow.getNumber("relationships"));
                })
        );

        List<Map<String, Object>> actual = new ArrayList<>();

        runQueryWithRowConsumer("CALL algo.graph.list() YIELD name, nodes, relationships, type, direction", resultRow -> {
            Map<String, Object> row = new HashMap<>();
            row.put("name", resultRow.getString("name"));
            row.put("type", resultRow.getString("type"));
            row.put("relationships", resultRow.getNumber("relationships"));
            row.put("nodes", resultRow.getNumber("nodes"));
            row.put("direction", resultRow.getString("direction"));

            actual.add(row);
        });

        assertEquals(parameters.get(0), actual.get(0));
    }

    @Disabled
    @Test
    void shouldListAllAvailableGraphsForUser() {
        String loadQuery = "CALL algo.graph.load(" +
                           "    $name, null, null, {}" +
                           ")" +
                           "YIELD nodes, relationships";

        runQuery("alice", loadQuery, MapUtil.map("name", "aliceGraph"));
        runQuery("bob", loadQuery, MapUtil.map("name", "bobGraph"));

        String listQuery = "CALL algo.graph.list() YIELD name, nodes, relationships, type, direction";

        runQueryWithRowConsumer("alice", listQuery, resultRow -> Assertions.assertEquals("aliceGraph", resultRow.getString("name")));
        runQueryWithRowConsumer("bob", listQuery, resultRow -> Assertions.assertEquals("bobGraph", resultRow.getString("name")));
    }

    @Disabled
    @ParameterizedTest
    @CsvSource(value = {
            "'CREATE (n) RETURN id(n) AS id', 'RETURN 0 AS source, 1 AS target'",
            "'RETURN 0 AS id', 'CREATE (n)-[:REL]->(m) RETURN id(n) AS source, id(m) AS target'"
    })
    void shouldFailToWriteInCypherLoaderQueries(String nodeQuery, String relQuery) {
        String query = String.format(
                "CALL algo.graph.load('dragons'," +
                "  '%s'," +
                "  '%s'," +
                "  {" +
                "    graph: 'cypher'" +
                "  })",
                nodeQuery, relQuery);
        QueryExecutionException ex = assertThrows(QueryExecutionException.class, () -> runQuery(query, Result::hasNext));
        Throwable root = ExceptionUtil.rootCause(ex);
        assertTrue(root instanceof IllegalArgumentException);
        assertThat(root.getMessage(), containsString("Query must be read only. Query: "));
    }

    @Disabled
    @Test
    void shouldPreferRelationshipPropertiesForCypherLoading() {
        String nodeQuery = ALL_NODES_QUERY.replaceAll("'", "");
        String relationshipQuery = "MATCH (s)-[r:Z]->(t) RETURN id(s) AS source, id(t) AS target " +
                                   " , 23 AS foo, 42 AS bar, 1984 AS baz, r.weight AS weight";

        String query = "CALL algo.graph.load(" +
                       "    'testGraph', $nodeQuery, $relationshipQuery, {" +
                       "        graph: 'cypher'," +
                       "        relationshipProperties: {" +
                       "            foobar : 'foo'," +
                       "            foobaz : 'baz'," +
                       "            raboof : 'weight'" +
                       "        }" +
                       "    }" +
                       ")";

        runQuery(query, MapUtil.map("nodeQuery", nodeQuery, "relationshipQuery", relationshipQuery));

        Graph foobarGraph = GraphCatalog.get(getUsername(), "testGraph", "", Optional.of("foobar"));
        Graph foobazGraph = GraphCatalog.get(getUsername(), "testGraph", "", Optional.of("foobaz"));
        Graph raboofGraph = GraphCatalog.get(getUsername(), "testGraph", "", Optional.of("raboof"));

        Graph expectedFoobarGraph = TestGraph.Builder.fromGdl("()-[{w: 23.0D}]->(),()-[{w: 23.0D}]->(),(),(),(),(),(),(),(),()");
        Graph expectedFoobazGraph = TestGraph.Builder.fromGdl("()-[{w: 1984.0D}]->(),()-[{w: 1984.0D}]->(),(),(),(),(),(),(),(),()");
        Graph expectedRaboofGraph = TestGraph.Builder.fromGdl("()-[{w: 1.0D}]->(),()-[{w: 42.0D}]->(),(),(),(),(),(),(),(),()");

        TestSupport.assertGraphEquals(expectedFoobarGraph, foobarGraph);
        TestSupport.assertGraphEquals(expectedFoobazGraph, foobazGraph);
        TestSupport.assertGraphEquals(expectedRaboofGraph, raboofGraph);
    }
}
