/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

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
public class LouvainClusteringIntegrationTest {

    private final static String[] NODES = {"a", "b", "c", "d", "e", "f", "g", "h", "z"};

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a', c:1})\n" +
                        "CREATE (c:Node {name:'c', c:1})\n" + // shuffled
                        "CREATE (b:Node {name:'b', c:1})\n" +
                        "CREATE (d:Node {name:'d', c:1})\n" +
                        "CREATE (e:Node {name:'e', c:1})\n" +
                        "CREATE (g:Node {name:'g', c:1})\n" +
                        "CREATE (f:Node {name:'f', c:1})\n" +
                        "CREATE (h:Node {name:'h', c:1})\n" +
                        "CREATE (z:Node {name:'z', c:1})\n" + // assign impossible community to outstanding node

                        "CREATE" +

                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (a)-[:TYPE]->(d),\n" +
                        " (c)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(d),\n" +

                        " (f)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(g),\n" +
                        " (e)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(h),\n" +
                        " (f)-[:TYPE]->(g),\n" +
                        " (g)-[:TYPE]->(h),\n" +
                        " (b)-[:TYPE]->(e)";

        DB.resolveDependency(Procedures.class).registerProcedure(LouvainProc.class);
        DB.execute(cypher);
    }

    @Before
    public void clearCommunities() {
        String cypher  ="MATCH (n) REMOVE n.communities REMOVE n.community";
        DB.execute(cypher);
    }

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    @Test
    public void test() {
        final String cypher = "CALL algo.louvain('', '', {concurrency:1}) " +
                "YIELD nodes, communityCount, loadMillis, computeMillis, writeMillis, postProcessingMillis, p99";

        DB.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("postProcessingMillis = " + row.getNumber("postProcessingMillis"));
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("p99 = " + row.get("p99"));

            assertEquals("invalid node count",9, nodes);
            assertEquals("wrong community count", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });
    }
    @Test
    public void testInnerIterations() {
        final String cypher = "CALL algo.louvain('', '', {concurrency:1, innerIterations:100}) " +
                "YIELD nodes, communityCount, loadMillis, computeMillis, writeMillis, postProcessingMillis, p99";

        DB.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("postProcessingMillis = " + row.getNumber("postProcessingMillis"));
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("p99 = " + row.get("p99"));

            assertEquals("invalid node count",9, nodes);
            assertEquals("wrong community count", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });
    }

    @Test
    public void testRandomNeighbor() {
        final String cypher = "CALL algo.louvain('', '', {concurrency:1, randomNeighbor:true}) " +
                "YIELD nodes, communityCount, loadMillis, computeMillis, writeMillis, postProcessingMillis, p99";

        DB.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("postProcessingMillis = " + row.getNumber("postProcessingMillis"));
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("p99 = " + row.get("p99"));

            assertEquals("invalid node count",9, nodes);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });
    }


    @Test
    public void testStream() {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:1, innerIterations:10, randomNeighbor:false}) " +
                "YIELD nodeId, community, communities";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).accept(row -> {
            final long nodeId = (long) row.get("nodeId");
            final long community = (long) row.get("community");
            System.out.println(nodeId + ": " + community);
            testMap.addTo((int) community, 1);
            return false;
        });
        assertEquals(3, testMap.size());
    }

    @Test
    public void testPredefinedCommunities() {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:1, communityProperty:'c'}) " +
                "YIELD nodeId, community, communities";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).accept(row -> {
            final long nodeId = (long) row.get("nodeId");
            final long community = (long) row.get("community");
            System.out.println(nodeId + ": " + community);
            testMap.addTo((int) community, 1);
            return false;
        });
        assertEquals(1, testMap.size());
    }

    @Test
    public void testStreamNoIntermediateCommunitiesByDefault() {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:1, communityProperty:'c'}) " +
                "YIELD nodeId, community, communities";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).accept(row -> {
            Object communities = row.get("communities");
            assertNull(communities);
            return false;
        });
    }

    @Test
    public void testStreamIncludingIntermediateCommunities() {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:1, includeIntermediateCommunities: true}) " +
                "YIELD nodeId, communities";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).accept(row -> {
            final long community = ((List<Long>) row.get("communities")).get(0);
            System.out.println(community);
            testMap.addTo((int) community, 1);
            return false;
        });
        assertEquals(3, testMap.size());
    }

    @Test
    public void testWrite() {
        final String cypher = "CALL algo.louvain('', '', {concurrency:1})";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).close();

        String readQuery = "MATCH (n) RETURN n.community AS community";

        DB.execute(readQuery).accept(row -> {
            final int community = (int) row.get("community");
            testMap.addTo(community, 1);
            return true;
        });

        assertEquals(3, testMap.size());
    }

    @Test
    public void testWriteIncludingIntermediateCommunities() {
        final String cypher = "CALL algo.louvain('', '', {concurrency:1, includeIntermediateCommunities: true})";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher).close();

        String readQuery = "MATCH (n) RETURN n.communities AS communities";

        DB.execute(readQuery).accept(row -> {
            final long community = ((int[]) row.get("communities"))[0];
            testMap.addTo((int) community, 1);
            return true;
        });

        assertEquals(3, testMap.size());
    }

    @Test
    public void testWriteNoIntermediateCommunitiesByDefault() {
        final String cypher = "CALL algo.louvain('', '', {concurrency:1})";
        DB.execute(cypher).close();

        final AtomicLong testInteger = new AtomicLong(0);
        String readQuery = "MATCH (n) WHERE not(exists(n.communities)) RETURN count(*) AS count";
        DB.execute(readQuery).accept(row -> {
            long count = (long) row.get("count");
            testInteger.set(count);
            return false;
        });

        assertEquals(9, testInteger.get());
    }

    @Test
    public void testWithLabelRel() {
        final String cypher = "CALL algo.louvain('Node', 'TYPE', {concurrency:1}) " +
                "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        DB.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long iterations = row.getNumber("iterations").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            System.out.println("iterations = " + iterations);
            assertEquals("invalid node count",9, nodes);
            assertEquals("wrong community count", 3, communityCount);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });

        printNodeSets();
    }

    @Test
    public void testWithWeight() {
        final String cypher = "CALL algo.louvain('Node', 'TYPE', {weightProperty:'w', defaultValue:1.0, concurrency:1}) " +
                "YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis";

        DB.execute(cypher).accept(row -> {
            final long nodes = row.getNumber("nodes").longValue();
            final long communityCount = row.getNumber("communityCount").longValue();
            final long iterations = row.getNumber("iterations").longValue();
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("nodes = " + nodes);
            System.out.println("communityCount = " + communityCount);
            assertEquals(3, communityCount);
            System.out.println("iterations = " + iterations);
            assertEquals("invalid node count", 9, nodes);
            assertTrue("invalid loadTime", loadMillis >= 0);
            assertTrue("invalid writeTime", writeMillis >= 0);
            assertTrue("invalid computeTime", computeMillis >= 0);
            return false;
        });

        printNodeSets();

    }

    @Test
    public void shouldAllowHeavyGraph() {
        final String cypher = "CALL algo.louvain('', '', {graph:'heavy'}) YIELD nodes, communityCount";

        DB.execute(cypher).accept(row -> {
            assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
            assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
            return true;
        });
    }

    @Test
    public void shouldAllowHugeGraph() {
        final String cypher = "CALL algo.louvain('', '', {graph:'huge'}) YIELD nodes, communityCount";

        DB.execute(cypher).accept(row -> {
            assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
            assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
            return true;
        });
    }

    @Test
    public void shouldAllowCypherGraph() {
        final String cypher = "CALL algo.louvain('MATCH (n) RETURN id(n) as id', 'MATCH (s)-->(t) RETURN id(s) as source, id(t) as target', {graph:'cypher'}) YIELD nodes, communityCount";

        DB.execute(cypher).accept(row -> {
            assertEquals("invalid node count",9, row.getNumber("nodes").longValue());
            assertEquals("wrong community count", 3, row.getNumber("communityCount").longValue());
            return true;
        });
    }

    public void printNodeSets() {
        final StringBuilder builder = new StringBuilder();
        for (String node : NODES) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(String.format("%s : %s", node, Arrays.toString(getClusterId(node))));
        }
        System.out.println(builder.toString());
    }


    public int[] getClusterId(String nodeName) {

        int id[] = {0};
        DB.execute("MATCH (n) WHERE n.name = '" + nodeName + "' RETURN n").accept(row -> {
            id[0] = (int) row.getNode("n").getProperty("community");
            return true;
        });
        return id;
    }
}
