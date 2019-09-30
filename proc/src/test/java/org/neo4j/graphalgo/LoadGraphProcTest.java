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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.helpers.collection.MapUtil;
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
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.graphalgo.GraphHelper.assertOutRelationships;
import static org.neo4j.graphalgo.GraphHelper.assertOutProperties;
import static org.neo4j.graphalgo.GraphHelper.assertOutPropertiesWithDelta;
import static org.neo4j.graphalgo.TestSupport.allGraphNames;
import static org.neo4j.graphalgo.TestSupport.allGraphNamesAndDirections;

class LoadGraphProcTest extends ProcTestBase {

    private static final String ALL_NODES_QUERY = "'MATCH (n) RETURN id(n) AS id'";
    private static final String ALL_RELATIONSIHPS_QUERY = "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'";
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

    private GraphDatabaseAPI db;

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LoadGraphProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(UnionFindProc.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        LoadGraphFactory.removeAllLoadedGraphs();
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldLoadGraph(String graphImpl) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String query = graphImpl.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        runQuery(query, db, singletonMap("graph", graphImpl),
                row -> {
                    assertEquals(12, row.getNumber("nodes").intValue());
                    assertEquals(10, row.getNumber("relationships").intValue());
                    assertEquals(graphImpl, row.getString("graph"));
                    assertFalse(row.getBoolean("alreadyLoaded"));
                }
        );
    }

    static Stream<Arguments> relationshipWeightParameters() {
        return allGraphNames()
                .flatMap(graphName -> Stream.of("relationshipWeight", "weightProperty")
                        .map(propertyParam -> arguments(graphName, propertyParam)));
    }

    @ParameterizedTest(name = "graphImpl = {0}, relationshipWeightParameter = {1}")
    @MethodSource("relationshipWeightParameters")
    void shouldLoadGraphWithRelationshipWeight(String graphImpl, String relationshipWeightParam) {
        String query = "CALL algo.graph.load(" +
                       "    'foo', '', '', {" +
                       "        graph: $graph, " + relationshipWeightParam + ": 'weight'" +
                       "    }" +
                       ")";

        runQuery(query, db, singletonMap("graph", graphImpl));

        Graph fooGraph = LoadGraphFactory.getUnion("foo");
        assertNotNull(fooGraph);
        assertEquals(12, fooGraph.nodeCount());
        assertEquals(10, fooGraph.relationshipCount());

        assertOutProperties(fooGraph, 0, 1.0, 1.0, 1.0, 1.0, 1.0);
        assertOutProperties(fooGraph, 1, 42.0, 42.0, 42.0, 42.0, 42.0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldLoadGraphWithSaturatedThreadPool(String graphImpl) {
        // ensure that we don't drop task that can't be scheduled while importing a graph.

        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph, batchSize: 2})";
        String query = graphImpl.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        List<Future<?>> futures = new ArrayList<>();
        // block all available threads
        for (int i = 0; i < Pools.DEFAULT_CONCURRENCY; i++) {
            futures.add(
                    Pools.DEFAULT.submit(() -> LockSupport.parkNanos(Duration.ofSeconds(1).toNanos()))
            );
        }

        try {
            runQuery(query, db, singletonMap("graph", graphImpl),
                    row -> {
                        assertEquals(12, row.getNumber("nodes").intValue());
                        assertEquals(10, row.getNumber("relationships").intValue());
                        assertEquals(graphImpl, row.getString("graph"));
                        assertFalse(row.getBoolean("alreadyLoaded"));
                    }
            );
        } finally {
            ParallelUtil.awaitTermination(futures);
        }
    }

    @Test
    void shouldLoadGraphWithMultipleRelationships() {
        String query = "CALL algo.graph.load(" +
                       "    'foo', null, 'X | Y', {" +
                       "        graph: 'huge'" +
                       "    }" +
                       ")";

        runQuery(
                query,
                db,
                row -> {
                    assertEquals(12, row.getNumber("nodes").intValue());
                    assertEquals(8, row.getNumber("relationships").intValue());
                    assertEquals("huge", row.getString("graph"));
                    assertFalse(row.getBoolean("alreadyLoaded"));
                }
        );
    }

    @AllGraphNamesTest
    void shouldFailToLoadGraphWithMultipleRelationships(String graphImpl) {
        String query = String.format("CALL algo.graph.load(" +
                                     "    'foo', 'null', 'X | Y', {" +
                                     "        graph: '%s'" +
                                     "    }" +
                                     ")", graphImpl);

        assertThrows(QueryExecutionException.class, () -> runQuery(query, db));
    }

    @Test
    void shouldLoadGraphWithMultipleRelationshipProperties() throws KernelException {
        GraphDatabaseAPI testLocalDb = TestDatabaseCreator.createTestDatabase();
        testLocalDb.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(LoadGraphProc.class);

        String testGraph =
                "CREATE" +
                "  (a: Node)" +
                ", (b: Node)" +
                ", (a)-[:TYPE_1 { weight: 42.1, cost: 1 }]->(b)" +
                ", (a)-[:TYPE_1 { weight: 43.2, cost: 2 }]->(b)" +
                ", (a)-[:TYPE_2 { weight: 44.3, cost: 3 }]->(b)" +
                ", (a)-[:TYPE_2 { weight: 45.4, cost: 4 }]->(b)";

        testLocalDb.execute(testGraph);

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

        runQuery(loadQuery, testLocalDb, row -> {
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

        Graph g = LoadGraphFactory.getUnion("aggGraph");

        assertEquals(2, g.nodeCount());
        assertEquals(3, g.relationshipCount());

        assertOutRelationships(g, 0, 1, 1, 1);
        assertOutPropertiesWithDelta(g, 1E-3, 0, 85.3, 42.1, 2.0);

        LoadGraphFactory.remove("aggGraph");
        testLocalDb.shutdown();
    }

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
            db.execute(loadQuery);
        });
        Throwable rootCause = ExceptionUtil.rootCause(exMissingProperty);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Relationship properties not found: 'cost'"));
    }

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
            db.execute(loadQuery);
        });
        Throwable rootCause = ExceptionUtil.rootCause(exMissingProperty);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Deduplication strategy `FOOBAR` is not supported."));
    }

    @Test
    void shouldComputeMemoryEstimationForHuge() {
        String query = "CALL algo.graph.load.memrec(" +
                       "    null, null, {" +
                       "        graph: $graph" +
                       "    }" +
                       ") YIELD bytesMin, bytesMax";
        runQuery(query, db, singletonMap("graph", "huge"),
                row -> {
                    assertEquals(303520, row.getNumber("bytesMin").longValue());
                    assertEquals(303520, row.getNumber("bytesMax").longValue());
                }
        );
    }

    @Test
    void shouldComputeMemoryEstimationForHugeWithProperties() {
        String query = "CALL algo.graph.load.memrec(" +
                       "    null, null, {" +
                       "        graph: $graph, weightProperty: 'weight'" +
                       "    }" +
                       ") YIELD bytesMin, bytesMax";

        runQuery(query, db, singletonMap("graph", "huge"),
                row -> {
                    assertEquals(573952, row.getNumber("bytesMin").longValue());
                    assertEquals(573952, row.getNumber("bytesMax").longValue());
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldUseLoadedGraph(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        runQuery(loadQuery, db, singletonMap("graph", graph));

        String algoQuery = "CALL algo.pageRank(" +
                           "    null, null, {" +
                           "        graph: $name, write: false" +
                           "    }" +
                           ")";
        runQuery(algoQuery, db, singletonMap("name", "foo"),
                row -> assertEquals(12, row.getNumber("nodes").intValue()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void multiUseLoadedGraph(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        runQuery(loadQuery, db, singletonMap("graph", graph));

        String algoQuery = "CALL algo.pageRank(" +
                           "    null, null, {" +
                           "        graph: $name, write: false" +
                           "    }" +
                           ")";
        runQuery(algoQuery, db, singletonMap("name", "foo"),
                row -> assertEquals(12, row.getNumber("nodes").intValue()));
        runQuery(algoQuery, db, singletonMap("name", "foo"),
                row -> assertEquals(12, row.getNumber("nodes").intValue()));
    }

    @Test
    void multiUseLoadedGraphWithMultipleRelationships() {
        String query = "CALL algo.graph.load(" +
                       "    'foo', null, 'X | Y', {" +
                       "        graph: 'huge'" +
                       "    }" +
                       ")";

        runQuery(
                query,
                db,
                row -> {
                    assertEquals(12, row.getNumber("nodes").intValue());
                    assertEquals(8, row.getNumber("relationships").intValue());
                    assertEquals("huge", row.getString("graph"));
                    assertFalse(row.getBoolean("alreadyLoaded"));
                }
        );

        String algoQuery = "CALL algo.unionFind(" +
                           "    null, $relType, {" +
                           "        graph: 'foo', write: false" +
                           "    }" +
                           ")";
        runQuery(algoQuery, db, singletonMap("relType", "X | Y"),
                row -> assertEquals(4, row.getNumber("communityCount").intValue()));

        runQuery(algoQuery, db, singletonMap("relType", "X"),
                row -> assertEquals(6, row.getNumber("communityCount").intValue()));

        runQuery(algoQuery, db, singletonMap("relType", "Y"),
                row -> assertEquals(10, row.getNumber("communityCount").intValue()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void shouldWorkWithLimitedTypes(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");
        runQuery(loadQuery, db, singletonMap("graph", graph));

        String algoQuery = "CALL algo.labelPropagation(" +
                           "    null, null,{" +
                           "        graph: $name, write: false" +
                           "    }" +
                           ")";
        try {
            runQuery(
                    algoQuery,
                    db,
                    singletonMap("name", "foo"),
                    row -> assertEquals(12, row.getNumber("nodes").intValue()));
        } catch (QueryExecutionException qee) {
            qee.printStackTrace();
            fail("Error using wrong graph type:" + qee.getMessage());
        }
    }

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
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");

        Map<String, Object> params = singletonMap("graph", graph);
        // First load succeeds
        assertFalse(db.execute(query, params).<Boolean>columnAs("loaded").next());
        // Second load throws exception
        QueryExecutionException exGraphAlreadyLoaded = assertThrows(QueryExecutionException.class, () -> db.execute(query, params).next());
        Throwable rootCause = ExceptionUtil.rootCause(exGraphAlreadyLoaded);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("A graph with name 'foo' is already loaded."));
    }


    static Stream<Arguments> graphImplAndDirectionAndComputeDegreeDistribution() {
        return Stream.of(true, false)
                .flatMap(computeDegreeDistribution -> allGraphNamesAndDirections()
                        .map(comb -> arguments(comb.get()[0], comb.get()[1], computeDegreeDistribution)));
    }

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

        db.execute(query, MapUtil.map("graph", graphImpl, "direction", loadDirection));

        runQuery(
                "CALL algo.graph.info('foo', $computeDegreeDistr)",
                db,
                Collections.singletonMap("computeDegreeDistr", computeDegreeDistribution),
                resultRow -> {
                    assertEquals(graphImpl.toLowerCase(), resultRow.getString("type"));
                    assertEquals(loadDirection, resultRow.getString("direction"));
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void removeGraph(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");
        runQuery(query, db, singletonMap("graph", graph));

        runQuery("CALL algo.graph.info($name, true)", db, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph.equals("cypher") ? "huge" : graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("exists"));
        });
        runQuery("CALL algo.graph.remove($name)", db, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph.equals("cypher") ? "huge" : graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("removed"));
        });
        runQuery("CALL algo.graph.info($name)", db, singletonMap("name", "foo"), row -> {
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @Test
    void removeGraphWithMultipleRelationshipTypes() {
        String query = "CALL algo.graph.load(" +
                       "    'foo', null, 'X | Y', {" +
                       "        graph: 'huge'" +
                       "    }" +
                       ")";
        runQuery(query, db);

        runQuery("CALL algo.graph.info($name, true)", db, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(8, row.getNumber("relationships").intValue());
            assertEquals("huge", row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("exists"));
        });
        runQuery("CALL algo.graph.remove($name)", db, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(8, row.getNumber("relationships").intValue());
            assertEquals("huge", row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("removed"));
        });
        runQuery("CALL algo.graph.info($name)", db, singletonMap("name", "foo"), row -> {
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @Test
    void removeMissingGraphIsNoOp() {
        runQuery("CALL algo.graph.remove($name)", db, singletonMap("name", "foo"), row -> {
            assertEquals(0, row.getNumber("nodes").intValue());
            assertEquals(0, row.getNumber("relationships").intValue());
            assertNull(row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("removed"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"huge", "cypher"})
    void degreeDistribution(String graph) {
        String queryTemplate = "CALL algo.graph.load(" +
                               "    'foo', %s, %s, {" +
                               "        graph: $graph" +
                               "    }" +
                               ")";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, ALL_NODES_QUERY, ALL_RELATIONSIHPS_QUERY)
                : String.format(queryTemplate, "null", "null");
        runQuery(query, db, singletonMap("graph", graph));

        runQuery("CALL algo.graph.info($name, true)", db, singletonMap("name", "foo"), row -> {
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

        runQuery("CALL algo.graph.info($name, false)", db, singletonMap("name", "foo"), row -> {
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

        runQuery("CALL algo.graph.info($name, {})", db, singletonMap("name", "foo"), row -> {
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

        runQuery("CALL algo.graph.info($name, null)", db, singletonMap("name", "foo"), row -> {
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
        runQuery(loadQuery, db, singletonMap("graph", graph));

        String infoQuery = graph.equals("cypher")
                ? "CALL algo.graph.info($name, {direction:'OUT'})"
                : "CALL algo.graph.info($name, {direction:'IN'})";
        runQuery(infoQuery, db, singletonMap("name", "foo"), row -> {
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

    @Test
    public void shouldReturnEmptyList() {
        assertEmptyResult("CALL algo.graph.list() YIELD name, nodes, relationships, type, direction", db);
    }

    @Test
    public void shouldListAllAvailableGraphs() {
        String loadQuery = "CALL algo.graph.load(" +
                           "    $name, null, null, {" +
                           "        graph: $type, direction: $direction" +
                           "    }" +
                           ")" +
                           "YIELD nodes, relationships";

        List<Map<String, Object>> parameters = Arrays.asList(
                MapUtil.map("name", "foo", "type", "huge", "direction", "OUTGOING")
        );

        parameters.forEach((parameter) -> runQuery(loadQuery, db, parameter, resultRow -> {
                    parameter.put("nodes", resultRow.getNumber("nodes"));
                    parameter.put("relationships", resultRow.getNumber("relationships"));
                })
        );

        List<Map<String, Object>> actual = new ArrayList<>();

        runQuery("CALL algo.graph.list() YIELD name, nodes, relationships, type, direction", db, resultRow -> {
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
}
