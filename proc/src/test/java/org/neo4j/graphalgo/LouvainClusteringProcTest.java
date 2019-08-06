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

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.Rule;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ExpectedException;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Graph:
 *
 * (a)-(b)---(e)-(f)
 *  | X |     | X |   (z)
 * (c)-(d)   (g)-(h)
 *
 * @author mknblch
 */
public class LouvainClusteringProcTest extends ProcTestBase {

    private static final String[] NODES = {"a", "b", "c", "d", "e", "f", "g", "h", "z"};
    private static final int[] NODE_CLUSTER_ID = {0, 0, 0, 0, 1, 1, 1, 1, 2};

    @BeforeAll
    public static void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        final String cypher = "CREATE " +
                "  (a:Node {name: 'a', c: 1})" +
                ", (c:Node {name: 'c', c: 1})" + // shuffled
                ", (b:Node {name: 'b', c: 1})" +
                ", (d:Node {name: 'd', c: 1})" +
                ", (e:Node {name: 'e', c: 1})" +
                ", (g:Node {name: 'g', c: 1})" +
                ", (f:Node {name: 'f', c: 1})" +
                ", (h:Node {name: 'h', c: 1})" +
                ", (z:Node {name: 'z', c: 1})" + // assign impossible community to outstanding node

                ", (a)-[:TYPE {weight: 0.5}]->(b)" +
                ", (a)-[:TYPE {weight: 1.5}]->(c)" +
                ", (a)-[:TYPE {weight: 0.5}]->(d)" +
                ", (c)-[:TYPE {weight: 1.5}]->(d)" +
                ", (b)-[:TYPE {weight: 0.5}]->(c)" +
                ", (b)-[:TYPE {weight: 1.5}]->(d)" +

                ", (f)-[:TYPE]->(e)" +
                ", (e)-[:TYPE]->(g)" +
                ", (e)-[:TYPE]->(h)" +
                ", (f)-[:TYPE]->(h)" +
                ", (f)-[:TYPE]->(g)" +
                ", (g)-[:TYPE]->(h)" +
                ", (b)-[:TYPE]->(e)";

        Procedures procedures = db.getDependencyResolver().resolveDependency(Procedures.class);
        procedures.registerProcedure(LouvainProc.class);
        procedures.registerProcedure(LoadGraphProc.class);
        db.execute(cypher);
    }

    @BeforeEach
    public void clearCommunities() {
        String cypher  ="MATCH (n) REMOVE n.communities REMOVE n.community";
        db.execute(cypher);
    }

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void test(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        concurrency: 1, graph: $graph" +
                       "    } " +
                       ") YIELD nodes, communityCount, loadMillis, computeMillis, writeMillis, postProcessingMillis, p99";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long nodes = row.getNumber("nodes").longValue();
                    long communityCount = row.getNumber("communityCount").longValue();
                    long loadMillis = row.getNumber("loadMillis").longValue();
                    long computeMillis = row.getNumber("computeMillis").longValue();
                    long writeMillis = row.getNumber("writeMillis").longValue();

                    assertEquals("invalid node count",9, nodes);
                    assertEquals("wrong community count", 3, communityCount);
                    assertTrue("invalid loadTime", loadMillis >= 0);
                    assertTrue("invalid writeTime", writeMillis >= 0);
                    assertTrue("invalid computeTime", computeMillis >= 0);
                }
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testInnerIterations(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        concurrency: 1, innerIterations: 100, graph: $graph" +
                       "    }" +
                       ") YIELD nodes, communityCount, loadMillis, computeMillis, writeMillis, postProcessingMillis, p99";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long nodes = row.getNumber("nodes").longValue();
                    long communityCount = row.getNumber("communityCount").longValue();
                    long loadMillis = row.getNumber("loadMillis").longValue();
                    long computeMillis = row.getNumber("computeMillis").longValue();
                    long writeMillis = row.getNumber("writeMillis").longValue();

                    assertEquals("invalid node count",9, nodes);
                    assertEquals("wrong community count", 3, communityCount);
                    assertTrue("invalid loadTime", loadMillis >= 0);
                    assertTrue("invalid writeTime", writeMillis >= 0);
                    assertTrue("invalid computeTime", computeMillis >= 0);
                }
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testRandomNeighbor(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        concurrency: 1, randomNeighbor: true, graph: $graph" +
                       "    }" +
                       ") YIELD nodes, communityCount, loadMillis, computeMillis, writeMillis, postProcessingMillis, p99";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long nodes = row.getNumber("nodes").longValue();
                    long communityCount = row.getNumber("communityCount").longValue();
                    long loadMillis = row.getNumber("loadMillis").longValue();
                    long computeMillis = row.getNumber("computeMillis").longValue();
                    long writeMillis = row.getNumber("writeMillis").longValue();

                    assertEquals("invalid node count",9, nodes);
                    assertEquals("wrong community count", 3, communityCount);
                    assertTrue("invalid loadTime", loadMillis >= 0);
                    assertTrue("invalid writeTime", writeMillis >= 0);
                    assertTrue("invalid computeTime", computeMillis >= 0);
                }
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testStream(String graphImpl) {
        String query = "CALL algo.louvain.stream(" +
                       "    '', '', {" +
                       "        concurrency: 1, innerIterations: 10, randomNeighbor: false, graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, community, communities";
        IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long community = (long) row.get("community");
                    testMap.addTo((int) community, 1);
                }
        );
        assertEquals(3, testMap.size());
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testPredefinedCommunities(String graphImpl) {
        Assumptions.assumeFalse(graphImpl.equalsIgnoreCase("kernel"));
        String query = "CALL algo.louvain.stream(" +
                       "    '', '', {" +
                       "        concurrency: 1, communityProperty: 'c', graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, community, communities";
        IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long community = (long) row.get("community");
                    testMap.addTo((int) community, 1);
                }
        );
        assertEquals(1, testMap.size());
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testStreamNoIntermediateCommunitiesByDefault(String graphImpl) {
        String query = "CALL algo.louvain.stream(" +
                       "    '', '', {" +
                       "        concurrency: 1, communityProperty: 'c', graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, community, communities";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    Object communities = row.get("communities");
                    assertNull(communities);
                }
        );
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testStreamIncludingIntermediateCommunities(String graphImpl) {
        String query = "CALL algo.louvain.stream(" +
                       "    '', '', {" +
                       "        concurrency: 1, includeIntermediateCommunities: true, graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, communities";
        IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long community = (Long) ((List) row.get("communities")).get(0);
                    testMap.addTo((int) community, 1);
                }
        );
        assertEquals(3, testMap.size());
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testWrite(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        concurrency: 1, graph: $graph" +
                       "    }" +
                       ")";
        runQuery(query, MapUtil.map("graph", graphImpl), (row) -> {});

        String readQuery = "MATCH (n) RETURN n.community AS community";

        IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(readQuery, row -> {
            int community = ((Number) row.get("community")).intValue();
            testMap.addTo(community, 1);
        });

        assertEquals(3, testMap.size());
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testWriteIncludingIntermediateCommunities(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        concurrency: 1, includeIntermediateCommunities: true, graph: $graph" +
                       "    }" +
                       ")";
        runQuery(query, MapUtil.map("graph", graphImpl), (row) -> {});

        String readQuery = "MATCH (n) RETURN n.communities AS communities";

        IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(readQuery, row -> {
            Object communities = row.get("communities");
            int community = Math.toIntExact(((long[]) communities)[0]);
            testMap.addTo(community, 1);
        });

        assertEquals(3, testMap.size());
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testWriteNoIntermediateCommunitiesByDefault(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        concurrency: 1, graph: $graph" +
                       "    }" +
                       ")";
        runQuery(query, MapUtil.map("graph", graphImpl), (row) -> {});

        String readQuery = "MATCH (n) " +
                           "WHERE not(exists(n.communities)) " +
                           "RETURN count(*) AS count";

        AtomicLong testInteger = new AtomicLong(0);
        runQuery(readQuery, row -> {
            long count = (long) row.get("count");
            testInteger.set(count);
        });

        assertEquals(9, testInteger.get());
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testWithLabelRel(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    'Node', 'TYPE', {" +
                       "        concurrency: 1, graph: $graph" +
                       "    }" +
                       ") YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long nodes = row.getNumber("nodes").longValue();
                    long communityCount = row.getNumber("communityCount").longValue();
                    long loadMillis = row.getNumber("loadMillis").longValue();
                    long computeMillis = row.getNumber("computeMillis").longValue();
                    long writeMillis = row.getNumber("writeMillis").longValue();

                    assertEquals("invalid node count",9, nodes);
                    assertEquals("wrong community count", 3, communityCount);
                    assertTrue("invalid loadTime", loadMillis >= 0);
                    assertTrue("invalid writeTime", writeMillis >= 0);
                    assertTrue("invalid computeTime", computeMillis >= 0);
                }
        );

        assertNodeSets();
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testWithWeight(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    'Node', 'TYPE', {" +
                       "        weightProperty: 'weight', defaultValue: 1.0, concurrency: 1, graph: $graph" +
                       "    }" +
                       ") YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long nodes = row.getNumber("nodes").longValue();
                    long communityCount = row.getNumber("communityCount").longValue();
                    long loadMillis = row.getNumber("loadMillis").longValue();
                    long computeMillis = row.getNumber("computeMillis").longValue();
                    long writeMillis = row.getNumber("writeMillis").longValue();

                    assertEquals(3, communityCount);
                    assertEquals("invalid node count", 9, nodes);
                    assertTrue("invalid loadTime", loadMillis >= 0);
                    assertTrue("invalid writeTime", writeMillis >= 0);
                    assertTrue("invalid computeTime", computeMillis >= 0);
                }
        );

        assertNodeSets();
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void shouldAllowHeavyGraph(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        graph: 'heavy'" +
                       "    }" +
                       ") YIELD nodes, communityCount";

        runQuery(query, row -> {
            assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
            assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
        });
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void shouldAllowHugeGraph(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        graph: 'huge'" +
                       "    }" +
                       ") YIELD nodes, communityCount";

        runQuery(query, row -> {
            assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
            assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
        });
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void shouldRunWithSaturatedThreadPool(String graphImpl) {
        // ensure that we don't drop task that can't be scheduled while executing the algorithm.

        // load graph first to isolate failing behavior to the actual algorithm execution.
        String loadQuery = "CALL algo.graph.load(" +
                           "    'louvainGraph', '', '', {" +
                           "        graph: $graph" +
                           "    }" +
                           ")";
        runQuery(loadQuery, MapUtil.map("graph", graphImpl), row -> {});

        List<Future<?>> futures = new ArrayList<>();
        // block all available threads
        for (int i = 0; i < Pools.DEFAULT_CONCURRENCY; i++) {
            futures.add(
                    Pools.DEFAULT.submit(() -> LockSupport.parkNanos(Duration.ofSeconds(1).toNanos()))
            );
        }

        String query = "CALL algo.louvain(" +
                       "    '', '', {" +
                       "        graph: 'louvainGraph'" +
                       "    }" +
                       ") YIELD nodes, communityCount";
        try {
            runQuery(query, row -> {
                assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
                assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
            });
        } finally {
            ParallelUtil.awaitTermination(futures);
        }
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void shouldAllowCypherGraph(String graphImpl) {
        String query = "CALL algo.louvain(" +
                       "    'MATCH (n) RETURN id(n) as id'," +
                       "    'MATCH (s)-->(t) RETURN id(s) as source, id(t) as target'," +
                       "     {graph: 'cypher'}" +
                       ") YIELD nodes, communityCount";

        runQuery(query, row -> {
            assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
            assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
        });
    }

    private void assertNodeSets() {
        for (int i = 0; i < NODES.length; i++) {
            String node = NODES[i];
            int expected = NODE_CLUSTER_ID[i];
            int clusterId = getClusterId(node);
            assertEquals(expected, clusterId);
        }
    }

    private int getClusterId(String nodeName) {
        int[] id = {-1};
        runQuery("MATCH (n) WHERE n.name = '" + nodeName + "' RETURN n", row -> {
            id[0] = ((Number) row.getNode("n").getProperty("community")).intValue();
        });
        return id[0];
    }
}
