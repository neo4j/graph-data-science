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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.AdditionalMatchers;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TriangleProc;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class ClusteringCoefficientIntegrationTest {

    private static GraphDatabaseAPI api;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (i:Node {name:'i'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (b)-[:TYPE]->(c),\n" +
                        " (c)-[:TYPE]->(a),\n" +

                        " (d)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(f),\n" +
                        " (f)-[:TYPE]->(d),\n" +

                        " (a)-[:TYPE]->(d),\n" +

                        " (g)-[:TYPE]->(h),\n" +
                        " (h)-[:TYPE]->(i),\n" +
                        " (i)-[:TYPE]->(g)";

        api = TestDatabaseCreator.createTestDatabase();

        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(TriangleProc.class);

        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        api.shutdown();
    }

    @Test
    public void testTriangleCountWriteCypher() throws Exception {
        final String cypher = "CALL algo.triangleCount('Node', '', {concurrency:4, write:true, clusterCoefficientProperty:'c'})";
        api.execute(cypher).accept(row -> {
            final long loadMillis = row.getNumber("loadMillis").longValue();
            final long computeMillis = row.getNumber("computeMillis").longValue();
            final long writeMillis = row.getNumber("writeMillis").longValue();
            final long nodeCount = row.getNumber("nodeCount").longValue();
            final long triangles = row.getNumber("triangleCount").longValue();
            final double coefficient = row.getNumber("averageClusteringCoefficient").doubleValue();
            final long p100 = row.getNumber("p100").longValue();

            assertNotEquals(-1, loadMillis);
            assertNotEquals(-1, computeMillis);
            assertNotEquals(-1, writeMillis);
            assertEquals(9, nodeCount);
            assertEquals(3, triangles);
            assertEquals(0.851, coefficient, 0.1);
            assertEquals(9, nodeCount);
            assertEquals(1, p100);
            return true;
        });

        final String request = "MATCH (n) WHERE exists(n.clusteringCoefficient) RETURN n.clusteringCoefficient as c";
        api.execute(request).accept(row -> {
            final double triangles = row.getNumber("c").doubleValue();
            System.out.println("triangles = " + triangles);
            return true;
        });
    }

    @Test
    public void testTriangleCountStream() throws Exception {
        final TriangleCountConsumer mock = mock(TriangleCountConsumer.class);
        final String cypher = "CALL algo.triangleCount.stream('Node', '', {concurrency:4}) YIELD nodeId, triangles, coefficient";
        api.execute(cypher).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final long triangles = row.getNumber("triangles").longValue();
            final double coefficient = row.getNumber("coefficient").doubleValue();
            mock.consume(nodeId, triangles, coefficient);
            return true;
        });
        verify(mock, times(7)).consume(anyLong(), eq(1L), AdditionalMatchers.eq(1.0, 0.1));
        verify(mock, times(2)).consume(anyLong(), eq(1L), AdditionalMatchers.eq(0.333, 0.1));
    }

    interface TriangleCountConsumer {
        void consume(long nodeId, long triangles, double value);
    }

}
