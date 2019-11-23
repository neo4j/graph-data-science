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

import com.carrotsearch.hppc.LongScatterSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ForwardBackwardSccProcTest extends ProcTestBase {

    @BeforeEach
    void setup() throws KernelException {
        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE (f:Node {name:'f'})\n" +
                "CREATE (g:Node {name:'g'})\n" +
                "CREATE (h:Node {name:'h'})\n" +
                "CREATE (i:Node {name:'i'})\n" +
                "CREATE (x:Node {name:'x'})\n" +
                "CREATE" +
                " (a)-[:TYPE {cost:5}]->(b),\n" +
                " (b)-[:TYPE {cost:5}]->(c),\n" +
                " (c)-[:TYPE {cost:5}]->(a),\n" +

                " (d)-[:TYPE {cost:2}]->(e),\n" +
                " (e)-[:TYPE {cost:2}]->(f),\n" +
                " (f)-[:TYPE {cost:2}]->(d),\n" +

                " (a)-[:TYPE {cost:2}]->(d),\n" +

                " (g)-[:TYPE {cost:3}]->(h),\n" +
                " (h)-[:TYPE {cost:3}]->(i),\n" +
                " (i)-[:TYPE {cost:3}]->(g)";

        db = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        registerProcedures(StronglyConnectedComponentsProc.class);
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    private long getNodeId(String name) {

        try (Transaction tx = db.beginTx()) {
            final long id = db.findNode(Label.label("Node"), "name", name).getId();
            tx.success();
            return id;
        }
    }

    @Test
    void testClusterA() {
        assertEquals(3, call(getNodeId("a")).size());
    }

    @Test
    void testClusterB() {
        assertEquals(3, call(getNodeId("d")).size());
    }

    @Test
    void testClusterC() {
        assertEquals(3, call(getNodeId("g")).size());
    }

    public LongScatterSet call(long nodeId) {
        String cypher = String.format("CALL algo.scc.forwardBackward.stream(%d, 'Node', 'TYPE', {concurrency:4}) YIELD nodeId RETURN nodeId", nodeId);
        final LongScatterSet set = new LongScatterSet();
        db.execute(cypher).accept(row -> {
            set.add(row.getNumber("nodeId").longValue());
            return true;
        });
        return set;
    }
}
