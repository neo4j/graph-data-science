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
import org.neo4j.graphalgo.PrimProc;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.junit.Assert.*;

/**
 *
 *         a                a
 *     1 /   \ 2          /  \
 *      /     \          /    \
 *     b --3-- c        b      c
 *     |       |   =>   |      |
 *     4       5        |      |
 *     |       |        |      |
 *     d --6-- e        d      e
 *
 *
 * @author mknblch
 */
public class PrimProcIntegrationTest {

    private static final RelationshipType type = RelationshipType.withName("TYPE");
    private static GraphDatabaseAPI db;

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    @BeforeClass
    public static void setup() throws KernelException {

        String cypher = "CREATE(a:Node {start:true}) " +
                "CREATE(b:Node) " +
                "CREATE(c:Node) " +
                "CREATE(d:Node) " +
                "CREATE(e:Node) " +
                "CREATE(z:Node) " +
                "CREATE (a)-[:TYPE {cost:1.0}]->(b) " +
                "CREATE (a)-[:TYPE {cost:2.0}]->(c) " +
                "CREATE (b)-[:TYPE {cost:3.0}]->(c) " +
                "CREATE (b)-[:TYPE {cost:4.0}]->(d) " +
                "CREATE (c)-[:TYPE {cost:5.0}]->(e) " +
                "CREATE (d)-[:TYPE {cost:6.0}]->(e)";

        db = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(PrimProc.class);
    }

    @Test
    public void testMinimum() throws Exception {

        db.execute("MATCH(n:Node{start:true}) WITH n CALL algo.spanningTree('Node', 'TYPE', 'cost', id(n), {graph:'huge', write:true, stats:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount " +
                "RETURN loadMillis, computeMillis, writeMillis, effectiveNodeCount").accept(res -> {

            System.out.println(res.get("loadMillis"));
            System.out.println(res.get("computeMillis"));
            System.out.println(res.get("writeMillis"));
            System.out.println(res.get("effectiveNodeCount"));

            assertNotEquals(-1L, res.getNumber("writeMillis").longValue());
            assertEquals(5, res.getNumber("effectiveNodeCount").intValue());

            return true;
        });

        final long relCount = db.execute("MATCH (a)-[:MST]->(b) RETURN id(a) as a, id(b) as b")
                .stream()
                .peek(m -> System.out.println(m.get("a") + " -> " + m.get("b")))
                .count();

        assertEquals(relCount, 4);
    }

    @Test
    public void testMaximum() throws Exception {

        db.execute("MATCH(n:Node{start:true}) WITH n CALL algo.spanningTree.maximum('Node', 'TYPE', 'cost', id(n), {writeProperty:'MAX', graph:'huge', write:true, stats:true}) " +
                "YIELD loadMillis, computeMillis, writeMillis, effectiveNodeCount " +
                "RETURN loadMillis, computeMillis, writeMillis, effectiveNodeCount").accept(res -> {

            System.out.println(res.get("loadMillis"));
            System.out.println(res.get("computeMillis"));
            System.out.println(res.get("writeMillis"));
            System.out.println(res.get("effectiveNodeCount"));

            assertNotEquals(-1L, res.getNumber("writeMillis").longValue());
            assertEquals(5, res.getNumber("effectiveNodeCount").intValue());

            return true;
        });

        final long relCount = db.execute("MATCH (a)-[:MAX]->(b) RETURN id(a) as a, id(b) as b")
                .stream()
                .peek(m -> System.out.println(m.get("a") + " -> " + m.get("b")))
                .count();

        assertEquals(relCount, 4);
    }
}
