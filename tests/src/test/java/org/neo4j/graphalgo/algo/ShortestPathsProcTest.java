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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Matchers;
import org.neo4j.graphalgo.ShortestPathsProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.DoubleConsumer;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**         5     5      5
 *      (A)---(B)---(C)----.
 *    5/ 2    2     2     2 \
 *  (S)---(G)---(H)---(I)---(X)
 *    3\    3     3     3   /
 *      (D)---(E)---(F)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */
final class ShortestPathsProcTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (s:Node {name: 's'})" +
            ", (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (f:Node {name: 'f'})" +
            ", (g:Node {name: 'g'})" +
            ", (h:Node {name: 'h'})" +
            ", (i:Node {name: 'i'})" +
            ", (x:Node {name: 'x'})" +

            ", (x)-[:TYPE {cost: 5}]->(s)" + // creates cycle

            ", (s)-[:TYPE {cost: 5}]->(a)" + // line 1
            ", (a)-[:TYPE {cost: 5}]->(b)" +
            ", (b)-[:TYPE {cost: 5}]->(c)" +
            ", (c)-[:TYPE {cost: 5}]->(x)" +

            ", (s)-[:TYPE {cost: 3}]->(d)" + // line 2
            ", (d)-[:TYPE {cost: 3}]->(e)" +
            ", (e)-[:TYPE {cost: 3}]->(f)" +
            ", (f)-[:TYPE {cost: 3}]->(x)" +

            ", (s)-[:TYPE {cost: 2}]->(g)" + // line 3
            ", (g)-[:TYPE {cost: 2}]->(h)" +
            ", (h)-[:TYPE {cost: 2}]->(i)" +
            ", (i)-[:TYPE {cost: 2}]->(x)";

    private static long startNode;
    private static long endNode;
    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setup() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ShortestPathsProc.class);

        try (Transaction tx = DB.beginTx()) {
            DB.execute(DB_CYPHER);
            startNode = DB.findNode(Label.label("Node"), "name", "s").getId();
            endNode = DB.findNode(Label.label("Node"), "name", "x").getId();
            tx.success();
        }
    }

    @AfterAll
    static void shutdownGraph() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphNamesTest
    void testResultStream(String graphName) {

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String cypher = "MATCH(n:Node {name:'s'}) " +
                              "WITH n CALL algo.shortestPaths.stream(n, 'cost',{graph:'" + graphName + "'}) " +
                              "YIELD nodeId, distance RETURN nodeId, distance";

        DB.execute(cypher).accept(row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double distance = row.getNumber("distance").doubleValue();
            consumer.accept(distance);
            System.out.printf(
                    "%d:%.1f, ",
                    nodeId,
                    distance);
            return true;
        });

        System.out.println();

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(1)).accept(eq(8D));
    }

    @AllGraphNamesTest
    void testWriteBack(String graphName) {

        final String matchCypher = "MATCH(n:Node {name:'s'}) " +
                                   "WITH n CALL algo.shortestPaths(n, 'cost', {write:true, writeProperty:'sp',graph:'" + graphName + "'}) " +
                                   "YIELD nodeCount, loadDuration, evalDuration, writeDuration RETURN nodeCount, loadDuration, evalDuration, writeDuration";

        DB.execute(matchCypher).accept(row -> {
            System.out.println("loadDuration = " + row.getNumber("loadDuration").longValue());
            System.out.println("evalDuration = " + row.getNumber("evalDuration").longValue());
            long writeDuration = row.getNumber("writeDuration").longValue();
            System.out.println("writeDuration = " + writeDuration);
            System.out.println("nodeCount = " + row.getNumber("nodeCount").longValue());
            assertNotEquals(-1L, writeDuration);
            return false;
        });

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String testCypher = "MATCH(n:Node) WHERE exists(n.sp) WITH n RETURN id(n) as id, n.sp as sp";

        DB.execute(testCypher).accept(row -> {
            double sp = row.getNumber("sp").doubleValue();
            consumer.accept(sp);
            return true;
        });

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(1)).accept(eq(8D));
    }

    @AllGraphNamesTest
    void testData(String graphName) {

        final Consumer mock = mock(Consumer.class);

        final String cypher = "MATCH(n:Node {name:'x'}) " +
                              "WITH n CALL algo.shortestPaths.stream(n, 'cost', {graph:'" + graphName + "'}) " +
                              "YIELD nodeId, distance RETURN nodeId, distance";

        DB.execute(cypher).accept(row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double distance = row.getNumber("distance").doubleValue();
            System.out.printf(
                    "%d:%.1f, ",
                    nodeId,
                    distance);
            mock.test(nodeId, distance);
            return true;
        });

        System.out.println();

        verify(mock, times(11)).test(anyLong(), anyDouble());
        verify(mock, times(1)).test(Matchers.eq(endNode), Matchers.eq(0.0));
        verify(mock, times(1)).test(Matchers.eq(startNode), Matchers.eq(5.0));
    }

    interface Consumer {
        void test(long source, double distance);
    }
}
