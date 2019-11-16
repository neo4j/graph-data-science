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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarity;
import org.neo4j.graphalgo.impl.jaccard.SimilarityResult;
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
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.TestGraph.Builder.fromGdl;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.crossArguments;
import static org.neo4j.graphalgo.TestSupport.toArguments;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

class NeighborhoodSimilarityProcTest extends ProcTestBase {

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

    private static final Collection<SimilarityResult> EXPECTED_OUTGOING = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_INCOMING = new HashSet<>();

    private static final Collection<SimilarityResult> EXPECTED_TOP_OUTGOING = new HashSet<>();
    private static final Collection<SimilarityResult> EXPECTED_TOP_INCOMING = new HashSet<>();

    static Stream<Arguments> allGraphNamesWithIncomingOutgoing() {
        return crossArguments(toArguments(TestSupport::allGraphNames), toArguments(() -> Stream.of(INCOMING, OUTGOING)));
    }

    static {
        EXPECTED_OUTGOING.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(0, 2, 1 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(1, 2, 0.0));
        // With mandatory topk, expect results in both directions
        EXPECTED_OUTGOING.add(new SimilarityResult(1, 0, 2 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(2, 0, 1 / 3.0));
        EXPECTED_OUTGOING.add(new SimilarityResult(2, 1, 0.0));

        EXPECTED_TOP_OUTGOING.add(new SimilarityResult(0, 1, 2 / 3.0));
        EXPECTED_TOP_OUTGOING.add(new SimilarityResult(1, 0, 2 / 3.0));

        EXPECTED_INCOMING.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(4, 6, 1 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(5, 6, 1 / 3.0));
        // With mandatory topk, expect results in both directions
        EXPECTED_INCOMING.add(new SimilarityResult(5, 4, 3.0 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(6, 4, 1 / 3.0));
        EXPECTED_INCOMING.add(new SimilarityResult(6, 5, 1 / 3.0));

        EXPECTED_TOP_INCOMING.add(new SimilarityResult(4, 5, 3.0 / 3.0));
        EXPECTED_TOP_INCOMING.add(new SimilarityResult(5, 4, 3.0 / 3.0));
    }

    @BeforeEach
    void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(DB_CYPHER);
        registerProcedures(NeighborhoodSimilarityProc.class);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    @ParameterizedTest(name = "{0} -- {1}")
    @MethodSource("allGraphNamesWithIncomingOutgoing")
    void shouldStreamResults(String graphImpl, Direction direction) {
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction" +
                       "    }" +
                       ") YIELD node1, node2, similarity";

        Collection<SimilarityResult> result = new HashSet<>();
        runQuery(query, db, MapUtil.map("graph", graphImpl, "direction", direction.name()),
                row -> {
                    long node1 = row.getNumber("node1").longValue();
                    long node2 = row.getNumber("node2").longValue();
                    double similarity = row.getNumber("similarity").doubleValue();
                    result.add(new SimilarityResult(node1, node2, similarity));
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
        int top = 2;
        String query = "CALL algo.beta.jaccard.stream(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction," +
                       "        top: $top" +
                       "    }" +
                       ") YIELD node1, node2, similarity";

        Collection<SimilarityResult> result = new HashSet<>();
        runQuery(query, db, MapUtil.map("graph", graphImpl, "direction", direction.name(), "top", top),
            row -> {
                long node1 = row.getNumber("node1").longValue();
                long node2 = row.getNumber("node2").longValue();
                double similarity = row.getNumber("similarity").doubleValue();
                result.add(new SimilarityResult(node1, node2, similarity));
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
    void shouldWriteResults(String graphImpl, Direction direction) throws KernelException {
        String query = "CALL algo.beta.jaccard(" +
                       "    '', 'LIKES', {" +
                       "        graph: $graph," +
                       "        direction: $direction" +
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
        Graph simGraph = GraphCatalog.getUnion(getUsername(), resultGraphName);
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

    @Test
    void shouldThrowIfTopKSetToZero() {
        Map<String, Object> input = MapUtil.map("topK", 0L);
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new NeighborhoodSimilarityProc().config(procedureConfiguration)
        );
        assertThat(illegalArgumentException.getMessage(), is("Must set non-zero topk value"));
    }

    @Test
    void shouldThrowIfDegreeCutoffSetToZero() {
        Map<String, Object> input = MapUtil.map("degreeCutoff", 0L);
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());

        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> new NeighborhoodSimilarityProc().config(procedureConfiguration)
        );
        assertThat(illegalArgumentException.getMessage(), is("Must set degree cutoff to 1 or greater"));
    }

    @Test
    void shouldCreateValidDefaultAlgoConfig() {
        Map<String, Object> input = MapUtil.map();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());
        NeighborhoodSimilarity.Config config = new NeighborhoodSimilarityProc().config(procedureConfiguration);

        assertEquals(10, config.topk());
        assertEquals(0, config.top());
        assertEquals(1, config.degreeCutoff());
        assertEquals(0.0, config.similarityCutoff());
        assertEquals(Pools.DEFAULT_CONCURRENCY, config.concurrency());
        assertEquals(ParallelUtil.DEFAULT_BATCH_SIZE, config.minBatchSize());
    }

    @Test
    void shouldCreateValidCustomAlgoConfig() {
        Map<String, Object> input = MapUtil.map(
            "topK", 100,
            "top", 1000,
            "degreeCutoff", 42,
            "similarityCutoff", 0.23,
            "concurrency", 1,
            "batchSize", 100_000
        );
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(input, getUsername());
        NeighborhoodSimilarity.Config config = new NeighborhoodSimilarityProc().config(procedureConfiguration);

        assertEquals(100, config.topk());
        assertEquals(1000, config.top());
        assertEquals(42, config.degreeCutoff());
        assertEquals(0.23, config.similarityCutoff());
        assertEquals(1, config.concurrency());
        assertEquals(100_000, config.minBatchSize());
    }
}
