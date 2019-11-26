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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.nodesim.NodeSimilarity;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.graphalgo.TestSupport.toArguments;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

class NodeSimilarityProcTest extends ProcTestBase {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Person {id: 0,  name: 'Alice'})" +
            ", (b:Person {id: 1,  name: 'Bob'})" +
            ", (c:Person {id: 2,  name: 'Charlie'})" +
            ", (d:Person {id: 3,  name: 'Dave'})" +
            ", (i1:Item  {id: 10, name: 'p1'})" +
            ", (i2:Item  {id: 11, name: 'p2'})" +
            ", (i3:Item  {id: 12, name: 'p3'})" +
            ", (i4:Item  {id: 13, name: 'p4'})" +
            ", (a)-[:LIKES]->(i1)" +
            ", (a)-[:LIKES]->(i2)" +
            ", (a)-[:LIKES]->(i3)" +
            ", (b)-[:LIKES]->(i1)" +
            ", (b)-[:LIKES]->(i2)" +
            ", (c)-[:LIKES]->(i3)";

    private static final Collection<String> EXPECTED_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_INCOMING = new HashSet<>();

    private static final Collection<String> EXPECTED_TOP_OUTGOING = new HashSet<>();
    private static final Collection<String> EXPECTED_TOP_INCOMING = new HashSet<>();

    static Stream<Arguments> allGraphNamesWithIncomingOutgoing() {
        return crossArguments(toArguments(TestSupport::allGraphNames), toArguments(() -> Stream.of(INCOMING, OUTGOING)));
    }

    static {
        EXPECTED_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(1, 2, 0.0));
        // With mandatory topK, expect results in both directions
        EXPECTED_OUTGOING.add(resultString(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING.add(resultString(2, 1, 0.0));

        EXPECTED_TOP_OUTGOING.add(resultString(0, 1, 2 / 3.0));
        EXPECTED_TOP_OUTGOING.add(resultString(1, 0, 2 / 3.0));

        EXPECTED_INCOMING.add(resultString(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING.add(resultString(4, 6, 1 / 3.0));
        EXPECTED_INCOMING.add(resultString(5, 6, 1 / 3.0));
        // With mandatory topK, expect results in both directions
        EXPECTED_INCOMING.add(resultString(5, 4, 3.0 / 3.0));
        EXPECTED_INCOMING.add(resultString(6, 4, 1 / 3.0));
        EXPECTED_INCOMING.add(resultString(6, 5, 1 / 3.0));

        EXPECTED_TOP_INCOMING.add(resultString(4, 5, 3.0 / 3.0));
        EXPECTED_TOP_INCOMING.add(resultString(5, 4, 3.0 / 3.0));
    }

    private static String resultString(long node1, long node2, double similarity) {
        return String.format("%d,%d %f%n", node1, node2, similarity);
    }

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
        registerProcedures(NodeSimilarityProc.class);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("allGraphNamesWithIncomingOutgoing")
    void shouldStreamResults(String graphImpl, Direction direction) {
        String query = "CALL algo.nodeSimilarity.stream(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction," +
                       "        similarityCutoff: 0.0" +
                       "    }" +
                       ") YIELD node1, node2, similarity";

        Collection<String> result = new HashSet<>();
        runQuery(query, db, MapUtil.map("graph", graphImpl, "direction", direction.name()),
                row -> {
                    long node1 = row.getNumber("node1").longValue();
                    long node2 = row.getNumber("node2").longValue();
                    double similarity = row.getNumber("similarity").doubleValue();
                    result.add(resultString(node1, node2, similarity));
                });

        assertEquals(
            direction == INCOMING
                ? EXPECTED_INCOMING
                : EXPECTED_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("allGraphNamesWithIncomingOutgoing")
    void shouldStreamTopResults(String graphImpl, Direction direction) {
        int topN = 2;
        String query = "CALL algo.nodeSimilarity.stream(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction," +
                       "        topN: $topN" +
                       "    }" +
                       ") YIELD node1, node2, similarity";

        Collection<String> result = new HashSet<>();
        runQuery(query, db, MapUtil.map("graph", graphImpl, "direction", direction.name(), "topN", topN),
            row -> {
                long node1 = row.getNumber("node1").longValue();
                long node2 = row.getNumber("node2").longValue();
                double similarity = row.getNumber("similarity").doubleValue();
                result.add(resultString(node1, node2, similarity));
            });

        assertEquals(
            direction == INCOMING
                ? EXPECTED_TOP_INCOMING
                : EXPECTED_TOP_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("allGraphNamesWithIncomingOutgoing")
    void shouldIgnoreParallelEdges(String graphImpl, Direction direction) {
        // Add parallel edges
        db.execute("" +
                   " MATCH (person {name: 'Alice'})" +
                   " MATCH (thing {name: 'p1'})" +
                   " CREATE (person)-[:LIKES]->(thing)"
        );
        db.execute("" +
                   " MATCH (person {name: 'Charlie'})" +
                   " MATCH (thing {name: 'p3'})" +
                   " CREATE (person)-[:LIKES]->(thing)" +
                   " CREATE (person)-[:LIKES]->(thing)" +
                   " CREATE (person)-[:LIKES]->(thing)"
        );

        String query = "CALL algo.nodeSimilarity.stream(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction," +
                       "        similarityCutoff: 0.0" +
                       "    }" +
                       ") YIELD node1, node2, similarity";

        Collection<String> result = new HashSet<>();
        runQuery(query, db, MapUtil.map("graph", graphImpl, "direction", direction.name()),
            row -> {
                long node1 = row.getNumber("node1").longValue();
                long node2 = row.getNumber("node2").longValue();
                double similarity = row.getNumber("similarity").doubleValue();
                result.add(resultString(node1, node2, similarity));
            });

        assertEquals(
            direction == INCOMING
                ? EXPECTED_INCOMING
                : EXPECTED_OUTGOING,
            result
        );
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("allGraphNamesWithIncomingOutgoing")
    void shouldWriteResults(String graphImpl, Direction direction) throws KernelException {
        String query = "CALL algo.nodeSimilarity(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction," +
                       "        similarityCutoff: 0.0" +
                       "    }" +
                       ") YIELD" +
                       " computeMillis," +
                       " loadMillis," +
                       " nodesCompared, " +
                       " relationships," +
                       " write," +
                       " writeMillis," +
                       " writeProperty," +
                       " writeRelationshipType," +
                       " min," +
                       " max," +
                       " mean," +
                       " stdDev," +
                       " p1," +
                       " p5," +
                       " p10," +
                       " p25," +
                       " p50," +
                       " p75," +
                       " p90," +
                       " p95," +
                       " p99," +
                       " p100," +
                       " postProcessingMillis";

        runQuery(query, MapUtil.map("graph", graphImpl, "direction", direction.name()),
            row -> {
                assertEquals(3, row.getNumber("nodesCompared").longValue());
                assertEquals(6, row.getNumber("relationships").longValue());
                assertEquals(true, row.getBoolean("write"));
                assertEquals("SIMILAR", row.getString("writeRelationshipType"));
                assertEquals("score", row.getString("writeProperty"));
                assertThat("Missing computeMillis", -1L, lessThan(row.getNumber("computeMillis").longValue()));
                assertThat("Missing loadMillis", -1L, lessThan(row.getNumber("loadMillis").longValue()));
                assertThat("Missing writeMillis", -1L, lessThan(row.getNumber("writeMillis").longValue()));
                assertThat("Missing min", -1.0, lessThan(row.getNumber("min").doubleValue()));
                assertThat("Missing max", -1.0, lessThan(row.getNumber("max").doubleValue()));
                assertThat("Missing mean", -1.0, lessThan(row.getNumber("mean").doubleValue()));
                assertThat("Missing stdDev", -1.0, lessThan(row.getNumber("stdDev").doubleValue()));
                assertThat("Missing p1", -1.0, lessThan(row.getNumber("p1").doubleValue()));
                assertThat("Missing p5", -1.0, lessThan(row.getNumber("p5").doubleValue()));
                assertThat("Missing p10", -1.0, lessThan(row.getNumber("p10").doubleValue()));
                assertThat("Missing p25", -1.0, lessThan(row.getNumber("p25").doubleValue()));
                assertThat("Missing p50", -1.0, lessThan(row.getNumber("p50").doubleValue()));
                assertThat("Missing p75", -1.0, lessThan(row.getNumber("p75").doubleValue()));
                assertThat("Missing p90", -1.0, lessThan(row.getNumber("p90").doubleValue()));
                assertThat("Missing p95", -1.0, lessThan(row.getNumber("p95").doubleValue()));
                assertThat("Missing p99", -1.0, lessThan(row.getNumber("p99").doubleValue()));
                assertThat("Missing p100", -1.0, lessThan(row.getNumber("p100").doubleValue()));
                assertThat("Missing postProcessingMillis", -1L, equalTo(row.getNumber("postProcessingMillis").longValue()));
            }
        );

        registerProcedures(GraphLoadProc.class);
        String resultGraphName = "simGraph_" + direction.name();
        String loadQuery = "CALL algo.graph.load($resultGraphName, $label, 'SIMILAR', {nodeProperties: 'id', relationshipProperties: 'score', direction: $direction})";
        db.execute(loadQuery, MapUtil.map("resultGraphName", resultGraphName, "label", direction == INCOMING ? "Item" : "Person", "direction", direction.name()));
        Graph simGraph = GraphCatalog.getUnion(getUsername(), resultGraphName).orElse(null);
        assertNotNull(simGraph);
        assertGraphEquals(direction == INCOMING
            ? fromGdl(
                String.format(
                    "  (i1 {id: 10})" +
                    ", (i2 {id: 11})" +
                    ", (i3 {id: 12})" +
                    ", (i4 {id: 13})" +
                    ", (i1)-[{w: %f}]->(i2)" +
                    ", (i1)-[{w: %f}]->(i3)" +
                    ", (i2)-[{w: %f}]->(i1)" +
                    ", (i2)-[{w: %f}]->(i3)" +
                    ", (i3)-[{w: %f}]->(i1)" +
                    ", (i3)-[{w: %f}]->(i2)",
                    1 / 1.0,
                    1 / 3.0,
                    1 / 1.0,
                    1 / 3.0,
                    1 / 3.0,
                    1 / 3.0
                )
            )
            : fromGdl(
                String.format(
                    "  (a {id: 0})" +
                    ", (b {id: 1})" +
                    ", (c {id: 2})" +
                    ", (d {id: 3})" +
                    ", (a)-[{w: %f}]->(b)" +
                    ", (a)-[{w: %f}]->(c)" +
                    ", (b)-[{w: %f}]->(c)" +
                    ", (b)-[{w: %f}]->(a)" +
                    ", (c)-[{w: %f}]->(a)" +
                    ", (c)-[{w: %f}]->(b)"
                    , 2 / 3.0
                    , 1 / 3.0
                    , 0.0
                    , 2 / 3.0
                    , 1 / 3.0
                    , 0.0
                )
            ),
            simGraph
        );

    }

    @ParameterizedTest(name = "parameter: {0}, value: {1}")
    @CsvSource(value = {"topN, -2", "bottomN, -2", "topK, -2", "bottomK, -2", "topK, 0", "bottomK, 0"})
    void shouldThrowForInvalidTopsAndBottoms(String parameter, long value) {
        String message = String.format("Invalid value for %s: must be a positive integer", parameter);
        Map<String, Object> input = MapUtil.map(parameter, value);
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new NodeSimilarityProc().config(procedureConfiguration)
        );
        assertThat(illegalArgumentException.getMessage(), containsString(message));
    }

    @ParameterizedTest
    @CsvSource(value = {"topK, bottomK", "topN, bottomN"})
    void shouldThrowForInvalidTopAndBottomCombination(String top, String bottom) {
        Map<String, Object> input = MapUtil.map(top, 1, bottom, 1);
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());

        String expectedMessage = String.format("Invalid parameter combination: %s combined with %s", top, bottom);

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new NodeSimilarityProc().config(procedureConfiguration)
        );
        assertThat(illegalArgumentException.getMessage(), is(expectedMessage));
    }

    @Test
    void shouldThrowIfDegreeCutoffSetToZero() {
        Map<String, Object> input = MapUtil.map("degreeCutoff", 0L);
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new NodeSimilarityProc().config(procedureConfiguration)
        );
        assertThat(illegalArgumentException.getMessage(), is("Must set degree cutoff to 1 or greater"));
    }

    @Test
    void shouldCreateValidDefaultAlgoConfig() {
        Map<String, Object> input = MapUtil.map();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());
        NodeSimilarity.Config config = new NodeSimilarityProc().config(procedureConfiguration);

        assertEquals(10, config.topK());
        assertEquals(0, config.topN());
        assertEquals(1, config.degreeCutoff());
        assertEquals(1E-42, config.similarityCutoff());
        assertEquals(Pools.DEFAULT_CONCURRENCY, config.concurrency());
        assertEquals(ParallelUtil.DEFAULT_BATCH_SIZE, config.minBatchSize());
    }

    @ParameterizedTest(name = "top or bottom: {0}")
    @ValueSource(strings = {"top", "bottom"})
    void shouldCreateValidCustomAlgoConfig(String parameter) {
        Map<String, Object> input = MapUtil.map(
            parameter + "K", 100,
            parameter + "N", 1000,
            "degreeCutoff", 42,
            "similarityCutoff", 0.23,
            "concurrency", 1,
            "batchSize", 100_000
        );
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());
        NodeSimilarity.Config config = new NodeSimilarityProc().config(procedureConfiguration);

        assertEquals(parameter.equals("top") ? 100 : -100, config.topK());
        assertEquals(parameter.equals("top") ? 1000 : -1000, config.topN());
        assertEquals(42, config.degreeCutoff());
        assertEquals(0.23, config.similarityCutoff());
        assertEquals(1, config.concurrency());
        assertEquals(100_000, config.minBatchSize());
    }
}
