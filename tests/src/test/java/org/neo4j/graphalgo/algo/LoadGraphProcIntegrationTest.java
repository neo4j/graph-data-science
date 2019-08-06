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
package org.neo4j.graphalgo.algo;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.LabelPropagationProc;
import org.neo4j.graphalgo.LoadGraphProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.core.loading.LoadGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@RunWith(Parameterized.class)
public class LoadGraphProcIntegrationTest {

    private static final String DB_CYPHER = "" +
            "CREATE (a:A {id: 0, partition: 42}) " +
            "CREATE (b:B {id: 1, partition: 42}) " +

            "CREATE (a)-[:X]->(:A {id: 2, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 3, weight: 2.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 4, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 5, weight: 1.0, partition: 1}) " +
            "CREATE (a)-[:X]->(:A {id: 6, weight: 8.0, partition: 2}) " +

            "CREATE (b)-[:X]->(:B {id: 7, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 8, weight: 2.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 9, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 10, weight: 1.0, partition: 1}) " +
            "CREATE (b)-[:X]->(:B {id: 11, weight: 8.0, partition: 2})";

    @Parameterized.Parameters(name = "graph={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"heavy"},
                new Object[]{"huge"},
                new Object[]{"cypher"},
                new Object[]{"kernel"}
        );
    }

    @Rule
    public ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    private final String graph;

    public LoadGraphProcIntegrationTest(String graph) {
        this.graph = graph;
    }

    @Before
    public void setup() throws KernelException {
        Procedures procedures = db.resolveDependency(Procedures.class);
        procedures.registerProcedure(LoadGraphProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        db.execute(DB_CYPHER);
    }

    @After
    public void tearDown() throws Exception {
        LoadGraphFactory.remove("foo");
    }

    @Test
    public void shouldLoadGraph() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph})";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");

        runQuery(query, singletonMap("graph", graph), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph, row.getString("graph"));
            assertFalse(row.getBoolean("alreadyLoaded"));
        });
    }

    @Test
    public void shouldLoadGraphWithSaturatedThreadPool() {
        // ensure that we don't drop task that can't be scheduled while importing a graph.

        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph, batchSize: 2})";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");

        List<Future<?>> futures = new ArrayList<>();
        // block all available threads
        for (int i = 0; i < Pools.DEFAULT_CONCURRENCY; i++) {
            futures.add(
                    Pools.DEFAULT.submit(() -> LockSupport.parkNanos(Duration.ofSeconds(1).toNanos()))
            );
        }

        try {
            runQuery(query, singletonMap("graph", graph), row -> {
                assertEquals(12, row.getNumber("nodes").intValue());
                assertEquals(10, row.getNumber("relationships").intValue());
                assertEquals(graph, row.getString("graph"));
                assertFalse(row.getBoolean("alreadyLoaded"));
            });
        } finally {
            ParallelUtil.awaitTermination(futures);
        }
    }

    @Test
    public void shouldComputeMemoryEstimationForHeavy() {
        Assume.assumeTrue(graph.equals("heavy"));

        String queryTemplate = "CALL algo.graph.load.memrec(%s, %s, {graph: $graph}) YIELD requiredMemory";
        String query = String.format(queryTemplate, "null", "null");

        runQuery(
                query,
                singletonMap("graph", graph),
                row -> assertEquals(MemoryUsage.humanReadable(992), row.getString("requiredMemory")));
    }

    @Test
    public void shouldComputeMemoryEstimationForHuge() {
        Assume.assumeTrue(graph.equals("huge"));

        String queryTemplate = "CALL algo.graph.load.memrec(%s, %s, {graph: $graph}) YIELD bytesMin, bytesMax";
        String query = String.format(queryTemplate, "null", "null");

        runQuery(
                query,
                singletonMap("graph", graph),
                row -> {
                    assertEquals(303528, row.getNumber("bytesMin").longValue());
                    assertEquals(303528, row.getNumber("bytesMax").longValue());
                });
    }

    @Test
    public void shouldUseLoadedGraph() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph})";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");

        db.execute(loadQuery, singletonMap("graph", graph)).close();

        String algoQuery = "CALL algo.pageRank(null,null,{graph:$name,write:false})";
        runQuery(algoQuery, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
        });
    }

    @Test
    public void multiUseLoadedGraph() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph})";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");

        db.execute(loadQuery, singletonMap("graph", graph)).close();

        String algoQuery = "CALL algo.pageRank(null,null,{graph:$name,write:false})";
        runQuery(algoQuery, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
        });
        runQuery(algoQuery, singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
        });
    }

    @Test
    public void shouldWorkWithLimitedTypes() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph})";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");
        db.execute(loadQuery, singletonMap("graph", graph)).close();

        String algoQuery = "CALL algo.labelPropagation(null,null,{graph:$name,write:false})";
        try {
            runQuery(algoQuery, singletonMap("name", "foo"), row -> {
                assertEquals(12, row.getNumber("nodes").intValue());
            });
        } catch (QueryExecutionException qee) {
            qee.printStackTrace();
            fail("Error using wrong graph type:" + qee.getMessage());
        }
    }

    @Test
    public void dontDoubleLoad() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph}) YIELD alreadyLoaded AS loaded RETURN loaded";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");

        Map<String, Object> params = singletonMap("graph", this.graph);
        assertFalse(db.execute(query, params).<Boolean>columnAs("loaded").next());
        assertTrue(db.execute(query, params).<Boolean>columnAs("loaded").next());
    }

    @Test
    public void removeGraph() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph})";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");
        db.execute(query, singletonMap("graph", graph)).close();

        runQuery("CALL algo.graph.info($name, true)", singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph.equals("cypher") ? "heavy" : graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("exists"));
        });
        runQuery("CALL algo.graph.remove($name)", singletonMap("name", "foo"), row -> {
            assertEquals(12, row.getNumber("nodes").intValue());
            assertEquals(10, row.getNumber("relationships").intValue());
            assertEquals(graph.equals("cypher") ? "heavy" : graph, row.getString("type"));
            assertEquals("foo", row.getString("name"));
            assertTrue(row.getBoolean("removed"));
        });
        runQuery("CALL algo.graph.info($name)", singletonMap("name", "foo"), row -> {
            assertEquals("foo", row.getString("name"));
            assertFalse(row.getBoolean("exists"));
        });
    }

    @Test
    public void degreeDistribution() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph})";
        String query = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)-->(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");
        db.execute(query, singletonMap("graph", graph)).close();

        runQuery("CALL algo.graph.info($name, true)", singletonMap("name", "foo"), row -> {
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

        runQuery("CALL algo.graph.info($name, false)", singletonMap("name", "foo"), row -> {
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

        runQuery("CALL algo.graph.info($name, {})", singletonMap("name", "foo"), row -> {
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

        runQuery("CALL algo.graph.info($name, null)", singletonMap("name", "foo"), row -> {
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

    @Test
    public void incomingDegreeDistribution() {
        String queryTemplate = "CALL algo.graph.load('foo', %s, %s, {graph: $graph, direction:'IN'})";
        String loadQuery = graph.equals("cypher")
                ? String.format(queryTemplate, "'MATCH (n) RETURN id(n) AS id'", "'MATCH (s)<--(t) RETURN id(s) AS source, id(t) AS target'")
                : String.format(queryTemplate, "null", "null");
        db.execute(loadQuery, singletonMap("graph", graph)).close();

        String infoQuery = graph.equals("cypher")
                ? "CALL algo.graph.info($name, {direction:'OUT'})"
                : "CALL algo.graph.info($name, {direction:'IN'})";
        runQuery(infoQuery, singletonMap("name", "foo"), row -> {
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

    private void runQuery(String query, Map<String, Object> params, Consumer<Result.ResultRow> check) {
        try (Result result = db.execute(query, params)) {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        }
    }
}
