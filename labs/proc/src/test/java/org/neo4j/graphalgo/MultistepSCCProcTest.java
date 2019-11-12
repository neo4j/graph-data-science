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

import com.carrotsearch.hppc.LongLongScatterMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
class MultistepSCCProcTest {

    private static GraphDatabaseAPI api;

    @BeforeAll
    static void setup() throws KernelException {
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

        api = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = api.beginTx()) {
            api.execute(cypher);
            tx.success();
        }


        api.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(StronglyConnectedComponentsProc.class);
    }

    @AfterAll
    static void shutdownGraph() {
        api.shutdown();
    }

    @Test
    void testWrite() {
        String cypher = "CALL algo.scc.multistep('Node', 'TYPE', {concurrency:4, cutoff:0}) " +
                "YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize " +
                "RETURN loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize";

        api.execute(cypher).accept(row -> {

            assertTrue(row.getNumber("loadMillis").longValue() > 0L);
            assertTrue(row.getNumber("computeMillis").longValue() > 0L);
            assertTrue(row.getNumber("writeMillis").longValue() > 0L);

            assertEquals(3, row.getNumber("setCount").intValue());
            assertEquals(3, row.getNumber("minSetSize").intValue());
            assertEquals(3, row.getNumber("maxSetSize").intValue());

            return true;
        });
    }

    @Test
    void testStream() {
        String cypher = "CALL algo.scc.multistep.stream('Node', 'TYPE', {write:true, concurrency:4, cutoff:0}) YIELD nodeId, cluster RETURN nodeId, cluster";
        final LongLongScatterMap testMap = new LongLongScatterMap();
        api.execute(cypher).accept(row -> {
            testMap.addTo(row.getNumber("cluster").longValue(), 1);
            return true;
        });
        // we expect 3 clusters
        assertEquals(3, testMap.size());
        // with 3 elements each
        testMap.forEach((Consumer<? super LongLongCursor>) cursor -> assertEquals(3, cursor.value));
    }
}
