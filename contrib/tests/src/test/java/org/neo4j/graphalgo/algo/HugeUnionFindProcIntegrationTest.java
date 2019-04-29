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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import com.neo4j.algo.UnionFindProc;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.*;

/**
 * @author mknblch
 */
public class HugeUnionFindProcIntegrationTest
{

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Label)\n" +
                "CREATE (nB:Label)\n" +
                "CREATE (nC:Label)\n" +
                "CREATE (nD:Label)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" + // {J}
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

    @Test
    public void testUnionFind() throws Exception {
        db.execute( "CALL algo.unionFind('', '',{graph:'Huge'}) YIELD setCount, communityCount")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3L, row.getNumber("communityCount"));
                    assertEquals(3L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testUnionFindWithLabel() throws Exception {
        db.execute( "CALL algo.unionFind('Label', '',{graph:'Huge'}) YIELD setCount, communityCount")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(1L, row.getNumber("communityCount"));
                    assertEquals(1L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testUnionFindWriteBack() throws Exception {
        db.execute( "CALL algo.unionFind('', 'TYPE', {write:true,graph:'Huge'}) YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty")
                .accept((Result.ResultVisitor<Exception>) row -> {
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
        db.execute( "CALL algo.unionFind('', 'TYPE', {write:true,graph:'Huge', writeProperty:'unionFind'}) YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty")
                .accept((Result.ResultVisitor<Exception>) row -> {
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
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute( "CALL algo.unionFind.stream('', 'TYPE', {graph:'Huge'}) YIELD setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 1, 2, 7);
    }

    @Test
    public void testThresholdUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute(
                "CALL algo.unionFind.stream('', 'TYPE', {weightProperty:'cost', defaultValue:10.0, threshold:5.0, concurrency:1, graph:'" +
                    "Huge" + "'}) YIELD setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    map.addTo(row.getNumber("setId").intValue(), 1);
                    return true;
                });
        assertMapContains(map, 4, 3, 2, 1);
    }

    @Test
    public void testThresholdUnionFindLowThreshold() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        db.execute( "CALL algo.unionFind.stream('', 'TYPE', {weightProperty:'cost', defaultValue:10.0, concurrency:1, threshold:3.14, graph:'" +
                    "Huge" + "'}) YIELD setId")
                .accept((Result.ResultVisitor<Exception>) row -> {
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
