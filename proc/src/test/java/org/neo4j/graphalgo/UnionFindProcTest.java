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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.unionfind.UnionFindProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class UnionFindProcTest {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Label { nodeId : 0, seedId: 42 })\n" +
                "CREATE (nB:Label { nodeId : 1, seedId: 42 })\n" +
                "CREATE (nC:Label { nodeId : 2, seedId: 42 })\n" +
                "CREATE (nD:Label { nodeId : 3, seedId: 42 })\n" +
                "CREATE (nE { nodeId : 4 })\n" +
                "CREATE (nF { nodeId : 5 })\n" +
                "CREATE (nG { nodeId : 6 })\n" +
                "CREATE (nH { nodeId : 7 })\n" +
                "CREATE (nI { nodeId : 8 })\n" +
                "CREATE (nJ { nodeId : 9 })\n" +
                "CREATE\n" +
                // {A, B, C, D}
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +
                "  (nD)-[:TYPE {cost:4.2}]->(nE),\n" + // threshold UF should split here
                // {E, F, G}
                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nG),\n" +
                // {H, I}
                "  (nH)-[:TYPE]->(nI)";

        db = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(createGraph).close();
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(UnionFindProc.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Huge"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testUnionFind() throws Exception {
        String query = "CALL algo.unionFind('', '', { graph: $graph }) " +
                       "YIELD setCount, communityCount";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testUnionFindWithLabel() throws Exception {
        String query = "CALL algo.unionFind('Label', '', { graph: $graph }) " +
                       "YIELD setCount, communityCount";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertEquals(1L, row.getNumber("communityCount"));
                    assertEquals(1L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testUnionFindWithSeed() throws Exception {
        Assume.assumeFalse(graphImpl.equalsIgnoreCase("kernel"));

        String query = "CALL algo.unionFind('', '', { graph: $graph, seedProperty: 'seedId' }) " +
                       "YIELD setCount, communityCount";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    return true;
                });

        db.execute("MATCH (n) RETURN n.partition AS partition").accept(
                row -> {
                    assertTrue(row.getNumber("partition").longValue() >= 42);
                    return true;
                }
        );

        db.execute("MATCH (n) RETURN n.nodeId AS nodeId, n.partition AS partition").accept(
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    final long partitionId = row.getNumber("partition").longValue();
                    if (nodeId >= 0 && nodeId <= 6) {
                        assertEquals(42, partitionId);
                    } else {
                        assertTrue(partitionId > 42);
                    }
                    return true;
                }
        );
    }

    @Test
    public void testUnionFindWithSeedAndConsecutive() throws Exception {
        Assume.assumeFalse(graphImpl.equalsIgnoreCase("kernel"));

        String query = "CALL algo.unionFind('', '', { graph: $graph, seedProperty: 'seedId', consecutiveIds: true }) " +
                       "YIELD setCount, communityCount";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    return true;
                });

        db.execute("MATCH (n) RETURN n.partition AS partition").accept(
                row -> {
                    assertThat(row.getNumber("partition").longValue(), greaterThanOrEqualTo(42L));
                    return true;
                }
        );

        db.execute("MATCH (n) RETURN n.nodeId AS nodeId, n.partition AS partition").accept(
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    final long partitionId = row.getNumber("partition").longValue();
                    if (nodeId >= 0 && nodeId <= 6) {
                        assertEquals(42, partitionId);
                    } else {
                        assertTrue(partitionId != 42);
                    }
                    return true;
                }
        );
    }

    @Test
    public void testUnionFindWithConsecutiveIds() throws Exception {
        String query = "CALL algo.unionFind('', '', { graph: $graph, consecutiveIds: true })";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    return true;
                });

        db.execute("MATCH (n) RETURN collect(distinct n.partition) AS partitions ").accept(
                row -> {

                    assertThat((List<Long>) row.get("partitions"), containsInAnyOrder(0L, 1L, 2L));
                    return true;
                }
        );
    }

    @Test
    public void testUnionFindWriteBack() throws Exception {
        String query = "CALL algo.unionFind('', 'TYPE', { write: true, graph: $graph }) " +
                       "YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    assertEquals("partition", row.getString("partitionProperty"));
                    assertEquals("partition", row.getString("writeProperty"));
                    return false;
                });
    }

    @Test
    public void testUnionFindWriteBackExplicitWriteProperty() throws Exception {
        String query = "CALL algo.unionFind('', 'TYPE', { write: true, graph: $graph, writeProperty: 'unionFind' }) " +
                       "YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty";

        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    assertNotEquals(-1L, row.getNumber("nodes"));
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    assertEquals("unionFind", row.getString("partitionProperty"));
                    assertEquals("unionFind", row.getString("writeProperty"));
                    return false;
                });
    }

    @Test
    public void testUnionFindStream() throws Exception {
        String query = "CALL algo.unionFind.stream('', 'TYPE', {graph: $graph}) YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 1, 2, 7);
    }

    @Test
    public void testThresholdUnionFindStream() throws Exception {
        String query = "CALL algo.unionFind.stream('', 'TYPE', " +
                       "{ weightProperty: 'cost', defaultValue: 10.0, threshold: 5.0, concurrency: 1, graph: $graph }) " +
                       "YIELD setId";

        IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 4, 3, 2, 1);
    }

    @Test
    public void testThresholdUnionFindLowThreshold() throws Exception {
        String query = "CALL algo.unionFind.stream('', 'TYPE', " +
                       "{ weightProperty: 'cost', defaultValue: 10.0, concurrency: 1, threshold: 3.14, graph: $graph }) " +
                       "YIELD setId";
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute(query, MapUtil.map("graph", graphImpl)).accept(
                (Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 1, 2, 7);
    }

    private static void assertMapContains(IntIntMap map, int... values) {
        assertEquals("set count does not match", values.length, map.size());
        for (int count : values) {
            assertTrue("set size " + count + " does not match", mapContainsValue(map, count));
        }
    }

    private static boolean mapContainsValue(IntIntMap map, int value) {
        for (IntIntCursor cursor : map) {
            if (cursor.value == value) {
                return true;
            }
        }
        return false;
    }
}
