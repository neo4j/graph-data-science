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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import org.neo4j.graphalgo.ShortestPathsProc;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
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
@RunWith(Parameterized.class)
public final class ShortestPathsProcTest {

    @ClassRule
    public static ImpermanentDatabaseRule api = new ImpermanentDatabaseRule();
    private static long startNode;
    private static long endNode;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (s:Node {name:'s'})\n" +
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

                        " (x)-[:TYPE {cost:5}]->(s),\n" + // creates cycle

                        " (s)-[:TYPE {cost:5}]->(a),\n" + // line 1
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(x),\n" +

                        " (s)-[:TYPE {cost:3}]->(d),\n" + // line 2
                        " (d)-[:TYPE {cost:3}]->(e),\n" +
                        " (e)-[:TYPE {cost:3}]->(f),\n" +
                        " (f)-[:TYPE {cost:3}]->(x),\n" +

                        " (s)-[:TYPE {cost:2}]->(g),\n" + // line 3
                        " (g)-[:TYPE {cost:2}]->(h),\n" +
                        " (h)-[:TYPE {cost:2}]->(i),\n" +
                        " (i)-[:TYPE {cost:2}]->(x)";

        api.resolveDependency(Procedures.class)
                .registerProcedure(ShortestPathsProc.class);

        api.executeAndCommit(__ -> {
            api.execute(cypher);
            startNode = api.findNode(Label.label("Node"), "name", "s").getId();
            endNode = api.findNode(Label.label("Node"), "name", "x").getId();
        });
    }

    @AfterClass
    public static void shutdownGraph() throws Exception {
        if (api != null) api.shutdown();
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Light"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testResultStream() throws Exception {

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String cypher = "MATCH(n:Node {name:'s'}) WITH n CALL algo.shortestPaths.stream(n, 'cost',{graph:'" + graphImpl + "'}) " +
                "YIELD nodeId, distance RETURN nodeId, distance";

        api.execute(cypher).accept(row -> {
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
        verify(consumer, times(1)).accept(eq(8d));
    }

    @Test
    public void testWriteBack() throws Exception {

        final String matchCypher = "MATCH(n:Node {name:'s'}) WITH n CALL algo.shortestPaths(n, 'cost', {write:true, writeProperty:'sp',graph:'" + graphImpl + "'}) " +
                "YIELD nodeCount, loadDuration, evalDuration, writeDuration RETURN nodeCount, loadDuration, evalDuration, writeDuration";

        api.execute(matchCypher).accept(row -> {
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

        api.execute(testCypher).accept(row -> {
            double sp = row.getNumber("sp").doubleValue();
            consumer.accept(sp);
            return true;
        });

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(1)).accept(eq(8d));
    }


    @Test
    public void testData() throws Exception {

        final Consumer mock = mock(Consumer.class);

        final String cypher = "MATCH(n:Node {name:'x'}) WITH n CALL algo.shortestPaths.stream(n, 'cost',{graph:'" + graphImpl + "'}) " +
                "YIELD nodeId, distance RETURN nodeId, distance";

        api.execute(cypher).accept(row -> {
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
