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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class StronglyConnectedComponentsProcIntegrationTest {

    private static final RelationshipType type = RelationshipType.withName("TYPE");
    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {

        String cypher = "CREATE(a:Node) " +
                "CREATE(b:Node) " +
                "CREATE(c:Node) " +
                "CREATE(d:Node) " +
                "CREATE(e:Node) " +
                // group 1
                "CREATE (a)-[:TYPE]->(b) " +
                "CREATE (a)<-[:TYPE]-(b) " +
                "CREATE (a)-[:TYPE]->(c) " +
                "CREATE (a)<-[:TYPE]-(c) " +
                "CREATE (b)-[:TYPE]->(c) " +
                "CREATE (b)<-[:TYPE]-(c) " +
                // group 2
                "CREATE (d)-[:TYPE]->(e) " +
                "CREATE (d)<-[:TYPE]-(e) " ;

        db = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(StronglyConnectedComponentsProc.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Huge"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testScc() throws Exception {
        db.execute("CALL algo.scc('Node', 'TYPE', {write:true, graph:'"+graphImpl+"'}) YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize, partitionProperty, writeProperty")
                .accept(row -> {
                    assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
                    assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
                    assertEquals(2, row.getNumber("setCount").longValue());
                    assertEquals(2, row.getNumber("minSetSize").longValue());
                    assertEquals(3, row.getNumber("maxSetSize").longValue());
                    assertEquals("partition", row.getString("partitionProperty"));
                    assertEquals("partition", row.getString("writeProperty"));

                    return true;
                });
    }

    @Test
    public void explicitWriteProperty() throws Exception {
        db.execute("CALL algo.scc('Node', 'TYPE', {write:true, graph:'"+graphImpl+"', writeProperty: 'scc'}) YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize, partitionProperty, writeProperty")
                .accept(row -> {
                    assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
                    assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
                    assertEquals(2, row.getNumber("setCount").longValue());
                    assertEquals(2, row.getNumber("minSetSize").longValue());
                    assertEquals(3, row.getNumber("maxSetSize").longValue());
                    assertEquals("scc", row.getString("partitionProperty"));
                    assertEquals("scc", row.getString("writeProperty"));

                    return true;
                });
    }
}
