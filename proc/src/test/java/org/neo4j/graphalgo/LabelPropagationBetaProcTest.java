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

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.TestSupport.SingleAndMultiThreadedAllGraphNames;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assume.assumeFalse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.helpers.collection.MapUtil.map;

class LabelPropagationBetaProcTest extends ProcTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            " (a:A {id: 0, seed: 42}) " +
            ",(b:B {id: 1, seed: 42}) " +

            ",(a)-[:X]->(:A {id: 2,  weight: 1.0, score: 1.0, seed: 1}) " +
            ",(a)-[:X]->(:A {id: 3,  weight: 2.0, score: 2.0, seed: 1}) " +
            ",(a)-[:X]->(:A {id: 4,  weight: 1.0, score: 1.0, seed: 1}) " +
            ",(a)-[:X]->(:A {id: 5,  weight: 1.0, score: 1.0, seed: 1}) " +
            ",(a)-[:X]->(:A {id: 6,  weight: 8.0, score: 8.0, seed: 2}) " +

            ",(b)-[:X]->(:B {id: 7,  weight: 1.0, score: 1.0, seed: 1}) " +
            ",(b)-[:X]->(:B {id: 8,  weight: 2.0, score: 2.0, seed: 1}) " +
            ",(b)-[:X]->(:B {id: 9,  weight: 1.0, score: 1.0, seed: 1}) " +
            ",(b)-[:X]->(:B {id: 10, weight: 1.0, score: 1.0, seed: 1}) " +
            ",(b)-[:X]->(:B {id: 11, weight: 8.0, score: 8.0, seed: 2})";

    @BeforeEach
    void setup() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();

        Procedures procedures = DB.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        procedures.registerProcedure(LoadGraphProc.class);

        DB.execute(DB_CYPHER);
    }

    static Stream<Arguments> successParameters() {
        return Stream.of(
                arguments("direction: 'BOTH'", "BOTH", 1),
                arguments("direction: 'BOTH'", "OUTGOING", 2),
                arguments("direction: 'BOTH'", "INCOMING", 2),
                arguments("direction: 'OUTGOING'", "OUTGOING", 2),
                arguments("direction: 'INCOMING'", "INCOMING", 2),
                arguments("undirected: true", "BOTH", 1)
        );
    }

    @ParameterizedTest
    @MethodSource("successParameters")
    void succeedingCombinationsForLoadedGraphs(String loadDirection, String runDirection, long expectedComponents) {
        String testGraph =
                "CREATE" +
                " (a:C) " +
                ",(b:C) " +
                ",(c:C) " +
                ",(d:C) " +

                ",(a)-[:Y]->(b) " +
                ",(c)-[:Y]->(b) " +
                ",(c)-[:Y]->(d) ";

        DB.execute(testGraph);
        DB.execute("CALL algo.graph.remove('myGraph')");

        String loadQuery = "CALL algo.graph.load(" +
                           "    'myGraph' ,'C', 'Y', {" +
                           "       graph: $graph, " + loadDirection +
                           "    }" +
                           ")";
        runQuery(loadQuery, DB, parParams(true, HugeGraph.TYPE));

        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, null, {" +
                       "        graph: 'myGraph', direction: $runDirection, write: false" +
                       "    }" +
                       ")";
        runQuery(query, DB, map("runDirection", runDirection),
                row -> {
                    assertEquals(1, row.getNumber("iterations").intValue());
                    assertFalse(row.getBoolean("write"));
                    assertEquals(expectedComponents, row.getNumber("communityCount").longValue());
                }
        );
    }

    static Stream<Arguments> failParameters() {
        return Stream.of(
                arguments("direction: 'OUTGOING'", "INCOMING"),
                arguments("direction: 'OUTGOING'", "BOTH"),
                arguments("direction: 'INCOMING'", "OUTGOING"),
                arguments("direction: 'INCOMING'", "BOTH"),
                arguments("undirected: true", "INCOMING"),
                arguments("undirected: true", "OUTGOING")
        );
    }

    @ParameterizedTest
    @MethodSource("failParameters")
    void failingCombinationsForLoadedGraphs(String loadDirection, String runDirection) {
        String testGraph =
                "CREATE" +
                " (a:C) " +
                ",(b:C) " +
                ",(c:C) " +
                ",(d:C) " +

                ",(a)-[:Y]->(b) " +
                ",(c)-[:Y]->(b) " +
                ",(c)-[:Y]->(d) ";

        DB.execute(testGraph);
        DB.execute("CALL algo.graph.remove('myGraph')");

        String loadQuery = "CALL algo.graph.load(" +
                           "    'myGraph' ,'C', 'Y', {" +
                           "       graph: $graph, " + loadDirection +
                           "    }" +
                           ")";
        runQuery(loadQuery, DB, parParams(true, HugeGraph.TYPE));

        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, null, {" +
                       "        graph: 'myGraph', direction: $runDirection, write: false" +
                       "    }" +
                       ")";
        String exceptionMessage = assertThrows(
                QueryExecutionException.class,
                () -> runQuery(query, DB, map("runDirection", runDirection))).getMessage();
        Assert.assertThat(exceptionMessage, Matchers.containsString("Incompatible directions between loaded graph and requested compute direction"));
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldTakeDifferentSeedProperties(boolean parallel, String graphName) {
        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, null, {" +
                       "        direction: 'OUTGOING', seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: 'lpa'" +
                       "    }" +
                       ")";

        runQuery(query, DB, parParams(parallel, graphName),
                row -> {
                    assertEquals(1, row.getNumber("iterations").intValue());
                    assertEquals("weight", row.getString("weightProperty"));
                    assertEquals("seed", row.getString("seedProperty"));
                    assertEquals("lpa", row.getString("writeProperty"));
                    assertTrue(row.getBoolean("write"));
                }
        );

        query = "CALL algo.beta.labelPropagation(" +
                "   null, null, {" +
                "       direction: 'OUTGOING', partitionProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: 'lpa'" +
                "   }" +
                ")";

        runQuery(query, DB, parParams(parallel, graphName),
                row -> {
                    assertEquals(1, row.getNumber("iterations").intValue());
                    assertEquals("weight", row.getString("weightProperty"));
                    assertEquals("seed", row.getString("seedProperty"));
                    assertEquals("lpa", row.getString("writeProperty"));
                    assertTrue(row.getBoolean("write"));
                }
        );
    }

    @SingleAndMultiThreadedAllGraphNames
    void explicitWriteProperty(boolean parallel, String graphName) {
        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, null, {" +
                       "        direction: 'OUTGOING', seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: 'lpa'" +
                       "    }" +
                       ")";

        runQuery(query, DB, parParams(parallel, graphName),
                row -> {
                    assertEquals(1, row.getNumber("iterations").intValue());
                    assertEquals("weight", row.getString("weightProperty"));
                    assertEquals("seed", row.getString("seedProperty"));
                    assertEquals("lpa", row.getString("writeProperty"));
                    assertTrue(row.getBoolean("write"));
                }
        );
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldTakeParametersFromConfig(boolean parallel, String graphName) {
        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, null, {" +
                       "        iterations: 5, write: false, weightProperty: 'score', seedProperty: $seedProperty" +
                       "    }" +
                       ")";

        runQuery(query, DB, parParams(parallel, graphName),
                row -> {
                    assertTrue(5 >= row.getNumber("iterations").intValue());
                    assertTrue(row.getBoolean("didConverge"));
                    assertFalse(row.getBoolean("write"));
                    assertEquals("score", row.getString("weightProperty"));
                    assertEquals("seed", row.getString("seedProperty"));
                }
        );
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldRunLabelPropagation(boolean parallel, String graphName) {
        assumeFalse(graphName.equalsIgnoreCase("kernel"));
        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, 'X', {" +
                       "        batchSize: $batchSize, direction: 'OUTGOING', concurrency: $concurrency, graph: $graph, seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: $writeProperty" +
                       "    }" +
                       ")";

        runQuery(query, DB, parParams(parallel, graphName),
                row -> {
                    assertEquals(12, row.getNumber("nodes").intValue());
                    assertTrue(row.getBoolean("write"));
                    assertTrue(row.getNumber("loadMillis").intValue() >= 0, "load time not set");
                    assertTrue(row.getNumber("computeMillis").intValue() >= 0, "compute time not set");
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        String check = "MATCH (n) " +
                       "WHERE n.id IN [0,1] " +
                       "RETURN n.community AS community";
        runQuery(check, DB, row -> assertEquals(2, row.getNumber("community").intValue()));
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldRunLabelPropagationWithoutInitialSeed(boolean parallel, String graphName) {
        assumeFalse(graphName.equalsIgnoreCase("kernel"));
        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, 'X', {" +
                       "        batchSize: $batchSize, direction: 'OUTGOING', concurrency: $concurrency, graph: $graph, weightProperty: $weightProperty, writeProperty: $writeProperty" +
                       "    }" +
                       ")";

        runQuery(query, DB, parParams(parallel, graphName),
                row -> {
                    assertNull(row.getString("seedProperty"));
                    assertEquals(12, row.getNumber("nodes").intValue());
                    assertTrue(row.getBoolean("write"));
                    assertTrue(row.getNumber("loadMillis").intValue() >= 0, "load time not set");
                    assertTrue(row.getNumber("computeMillis").intValue() >= 0, "compute time not set");
                    assertTrue(row.getNumber("writeMillis").intValue() >= 0, "write time not set");
                }
        );
        runQuery(
                "MATCH (n) WHERE n.id = 0 RETURN n.community AS community",
                DB, row -> assertEquals(6, row.getNumber("community").intValue())
        );
        runQuery(
                "MATCH (n) WHERE n.id = 1 RETURN n.community AS community",
                DB, row -> assertEquals(11, row.getNumber("community").intValue())
        );
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldThrowExceptionWhenInitialSeedDoesNotExists(boolean parallel, String graphName) {
        String query = "CALL algo.beta.labelPropagation(" +
                       "    null, null, {" +
                       "        seedProperty: $seedProperty" +
                       "    }" +
                       ")";

        QueryExecutionException exception = assertThrows(QueryExecutionException.class, () -> {
            Map<String, Object> params = parParams(parallel, graphName);
            params.put("seedProperty", "does_not_exist");
            runQuery(query, DB, params, row -> {});
        });
        Throwable rootCause = ExceptionUtil.rootCause(exception);
        assertEquals("Node property not found: 'does_not_exist'", rootCause.getMessage());
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldFilterByLabel(boolean parallel, String graphName) {
        String query = "CALL algo.beta.labelPropagation(" +
                       "    'A', 'X', {" +
                       "        batchSize: $batchSize, direction: 'OUTGOING', concurrency: $concurrency, graph: $graph, writeProperty: $writeProperty" +
                       "    }" +
                       ")";
        String checkA = "MATCH (n) WHERE n.id = 0 RETURN n.community AS community";
        String checkB = "MATCH (n) WHERE n.id = 1 RETURN n.community AS community";

        runQuery(query, DB, parParams(parallel, graphName));
        runQuery(checkA, DB, row -> assertEquals(2, row.getNumber("community").intValue()));
        runQuery(checkB, DB, row -> assertNull(row.getNumber("community")));
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldPropagateIncoming(boolean parallel, String graphName) {
        assumeFalse(graphName.equalsIgnoreCase("kernel"));
        String query = "CALL algo.beta.labelPropagation(" +
                       "    'A', 'X', {" +
                       "        batchSize: $batchSize, direction: 'INCOMING', concurrency: $concurrency, graph: $graph, seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: $writeProperty" +
                       "    }" +
                       ")";
        String check = "MATCH (n:A) " +
                       "WHERE n.id <> 0 " +
                       "RETURN n.community AS community";

        runQuery(query, DB, parParams(parallel, graphName));
        runQuery(check, DB, row -> assertEquals(42, row.getNumber("community").intValue()));
    }

    @SingleAndMultiThreadedAllGraphNames
    public void shouldRunOnUndirected(boolean parallel, String graphName) {
        assumeFalse(graphName.equalsIgnoreCase("kernel"));
        String query = "CALL algo.beta.labelPropagation(" +
                       "    'A', 'X', {" +
                       "        batchSize: $batchSize, direction: 'BOTH', concurrency: $concurrency, graph: $graph, seedProperty: $seedProperty, weightProperty: $weightProperty, writeProperty: $writeProperty" +
                       "    }" +
                       ")";
        String check = "MATCH (n:A) " +
                       "WHERE n.id <> 0 " +
                       "RETURN n.community AS community";

        runQuery(query, DB, parParams(parallel, graphName));
        runQuery(check, DB, row -> {
            final int community = row.getNumber("community").intValue();
            assertTrue(community == 2 || community == 42); // this is due to flaky behaviour in the undirected case
        });
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldAllowCypherGraph(boolean parallel, String graphName) {
        String query = "CALL algo.beta.labelPropagation(" +
                       "    'MATCH (n) RETURN id(n) AS id, n.weight AS weight, n.seed AS value', " +
                       "    'MATCH (s)-[r:X]->(t) RETURN id(s) AS source, id(t) AS target, r.weight AS weight', {" +
                       "        graph: 'cypher', direction: 'OUTGOING', batchSize: $batchSize, concurrency: $concurrency, writeProperty: $writeProperty" +
                       "    }" +
                       ")";
        runQuery(query, DB, parParams(parallel, graphName), row -> assertEquals(12, row.getNumber("nodes").intValue()));
    }

    @SingleAndMultiThreadedAllGraphNames
    void shouldStreamResults(boolean parallel, String graphName) {
        // this one deliberately tests the streaming and non streaming versions against each other to check we get the same results
        // we intentionally start with no labels defined for any nodes (hence seedProperty = {lpa, lpa2})

        String writingQuery = "CALL algo.beta.labelPropagation(" +
                              "    null, null, {" +
                              "       iterations: 20, direction: 'OUTGOING', writeProperty: 'lpa', weightProperty: $weightProperty" +
                              "    }" +
                              ")";
        runQuery(writingQuery, DB, parParams(parallel, graphName));

        String streamingQuery = "CALL algo.labelPropagation.stream(" +
                                "    null, null, {" +
                                "        iterations: 20, direction: 'OUTGOING', weightProperty: $weightProperty" +
                                "    }" +
                                ") YIELD nodeId, label " +
                                "MATCH (node) " +
                                "    WHERE id(node) = nodeId " +
                                "RETURN node.id AS id, id(node) AS internalNodeId, node.lpa AS seedProperty, label AS community";

        runQuery(streamingQuery, DB, parParams(parallel, graphName),
                row -> assertEquals(row.getNumber("seedProperty").intValue(), row.getNumber("community").intValue()));
    }

    @Test
    void testGeneratedAndProvidedLabelsDontConflict() throws KernelException {
        GraphDatabaseAPI DB = TestDatabaseCreator.createTestDatabase();
        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LabelPropagationProc.class);

        int seededLabel = 1;
        // When
        String query = "CREATE " +
                       " (a:Pet {id: 0,type: 'cat',   seedId: $seed}) " +
                       ",(b:Pet {id: 1,type: 'okapi', seedId: $seed}) " +
                       ",(c:Pet {id: 2,type: 'koala'}) " +
                       ",(d:Pet {id: 3,type: 'python'}) " +
                       ",(a)<-[:REL]-(c) " +
                       ",(b)<-[:REL]-(c) " +
                       "RETURN id(d) AS maxId";

        // (c) will get seed 1
        // (d) will get seed id(d) + 1
        Result initResult = DB.execute(query, Collections.singletonMap("seed", seededLabel));
        long maxId = Iterators.single(initResult.<Number>columnAs("maxId")).longValue();

        String lpa = "CALL algo.beta.labelPropagation.stream('Pet', 'REL', {seedProperty: 'seedId'}) " +
                     "YIELD nodeId, community " +
                     "MATCH (pet:Pet) WHERE id(pet) = nodeId " +
                     "RETURN pet.id AS nodeId, community";

        long[] sets = new long[4];
        DB.execute(lpa).accept(row -> {
            int nodeId = row.getNumber("nodeId").intValue();
            long setId = row.getNumber("community").longValue();
            sets[nodeId] = setId;
            return true;
        });

        long newLabel = maxId + seededLabel + 1;
        assertArrayEquals(new long[]{1, 1, 1, newLabel}, sets, "Incorrect result assuming initial labels are neo4j id");
    }

    private Map<String, Object> parParams(boolean parallel, String graphName) {
        return map(
                "batchSize", parallel ? 1 : 100,
                "concurrency", parallel ? 8 : 1,
                "graph", graphName,
                "seedProperty", "seed",
                "weightProperty", "weight",
                "writeProperty", "community"
        );
    }
}
