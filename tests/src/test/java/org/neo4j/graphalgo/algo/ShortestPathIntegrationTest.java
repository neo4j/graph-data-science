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
/**
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

import org.hamcrest.Matcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class ShortestPathIntegrationTest {

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Node{type:'start'})\n" + // start
                        "CREATE (nB:Node)\n" +
                        "CREATE (nC:Node)\n" +
                        "CREATE (nD:Node)\n" +
                        "CREATE (nX:Node{type:'end'})\n" + // end
                        "CREATE\n" +

                        // sum: 9.0
                        "  (nA)-[:TYPE {cost:9.0}]->(nX),\n" +
                        // sum: 8.0
                        "  (nA)-[:TYPE {cost:4.0}]->(nB),\n" +
                        "  (nB)-[:TYPE {cost:4.0}]->(nX),\n" +
                        // sum: 6
                        "  (nA)-[:TYPE {cost:2.0}]->(nC),\n" +
                        "  (nC)-[:TYPE {cost:2.0}]->(nD),\n" +
                        "  (nD)-[:TYPE {cost:2.0}]->(nX)";

        DB.execute(createGraph).close();
        DB.resolveDependency(Procedures.class).registerProcedure(ShortestPathProc.class);
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
    public void noWeightStream() throws Exception {
        PathConsumer consumer = mock(PathConsumer.class);
        DB.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath.stream(start, end) " +
                        "YIELD nodeId, cost RETURN nodeId, cost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.accept((Long) row.getNumber("nodeId"), (Double) row.getNumber("cost"));
                    return true;
                });
        verify(consumer, times(2)).accept(anyLong(), anyDouble());
        verify(consumer, times(1)).accept(anyLong(), eq(0.0));
        verify(consumer, times(1)).accept(anyLong(), eq(1.0));
    }

    @Test
    public void noWeightWrite() throws Exception {
        DB.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath(start, end) " +
                        "YIELD loadMillis, evalMillis, writeMillis, nodeCount, totalCost\n" +
                        "RETURN loadMillis, evalMillis, writeMillis, nodeCount, totalCost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(1.0, (Double) row.getNumber("totalCost"), 0.01);
                    assertEquals(2L, row.getNumber("nodeCount"));
                    assertNotEquals(-1L, row.getNumber("loadMillis"));
                    assertNotEquals(-1L, row.getNumber("evalMillis"));
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    return false;
                });

        final StepConsumer mock = mock(StepConsumer.class);

        DB.execute("MATCH (n) WHERE exists(n.sssp) RETURN id(n) as id, n.sssp as sssp")
                .accept(row -> {
                    mock.accept(
                            row.getNumber("id").longValue(),
                            row.getNumber("sssp").doubleValue());
                    return true;
                });

        verify(mock, times(2)).accept(anyLong(), anyDouble());

        verify(mock, times(1)).accept(anyLong(), eq(0.0));
        verify(mock, times(1)).accept(anyLong(), eq(1.0));
    }

    @Test
    public void testDijkstraStream() throws Exception {
        PathConsumer consumer = mock(PathConsumer.class);
        DB.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath.stream(start, end, 'cost',{graph:'" + graphImpl + "'}) " +
                        "YIELD nodeId, cost RETURN nodeId, cost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.accept((Long) row.getNumber("nodeId"), (Double) row.getNumber("cost"));
                    return true;
                });
        verify(consumer, times(4)).accept(anyLong(), anyDouble());
        verify(consumer, times(1)).accept(anyLong(), eq(0.0));
        verify(consumer, times(1)).accept(anyLong(), eq(2.0));
        verify(consumer, times(1)).accept(anyLong(), eq(4.0));
        verify(consumer, times(1)).accept(anyLong(), eq(6.0));
    }

    @Test
    public void testDijkstra() throws Exception {
        DB.execute(
                "MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath(start, end, 'cost',{graph:'" + graphImpl + "', write:true, writeProperty:'step'}) " +
                        "YIELD loadMillis, evalMillis, writeMillis, nodeCount, totalCost\n" +
                        "RETURN loadMillis, evalMillis, writeMillis, nodeCount, totalCost")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3.0, (Double) row.getNumber("totalCost"), 10E2);
                    assertEquals(4L, row.getNumber("nodeCount"));
                    assertNotEquals(-1L, row.getNumber("loadMillis"));
                    assertNotEquals(-1L, row.getNumber("evalMillis"));
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    return false;
                });

        final StepConsumer mock = mock(StepConsumer.class);

        DB.execute("MATCH (n) WHERE exists(n.step) RETURN id(n) as id, n.step as step")
                .accept(row -> {
                    mock.accept(
                            row.getNumber("id").longValue(),
                            row.getNumber("step").doubleValue());
                    return true;
                });

        verify(mock, times(4)).accept(anyLong(), anyDouble());

        verify(mock, times(1)).accept(anyLong(), eq(0.0));
        verify(mock, times(1)).accept(anyLong(), eq(2.0));
        verify(mock, times(1)).accept(anyLong(), eq(4.0));
        verify(mock, times(1)).accept(anyLong(), eq(6.0));
    }

    private interface PathConsumer {
        void accept(long nodeId, double cost);
    }

    interface StepConsumer {
        void accept(long nodeId, double step);
    }
}
