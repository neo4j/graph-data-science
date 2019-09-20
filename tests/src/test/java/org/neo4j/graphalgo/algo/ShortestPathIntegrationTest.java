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
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ShortestPathIntegrationTest {

    private static final String DB_CYPHER =
            "CREATE" +
            "  (nA:Node {type: 'start'})" + // start
            ", (nB:Node)" +
            ", (nC:Node)" +
            ", (nD:Node)" +
            ", (nX:Node {type: 'end'})" + // end
            // sum: 9.0
            ", (nA)-[:TYPE {cost: 9.0}]->(nX)" +
            // sum: 8.0
            ", (nA)-[:TYPE {cost: 4.0}]->(nB)" +
            ", (nB)-[:TYPE {cost: 4.0}]->(nX)" +
            // sum: 6
            ", (nA)-[:TYPE {cost: 2.0}]->(nC)" +
            ", (nC)-[:TYPE {cost: 2.0}]->(nD)" +
            ", (nD)-[:TYPE {cost: 2.0}]->(nX)";

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setup() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(DB_CYPHER);
        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(ShortestPathProc.class);
    }

    @AfterAll
    static void shutdown() {
        if (DB != null) {
            DB.shutdown();
        }
    }

    @AllGraphNamesTest
    void noWeightStream(String graphName) throws Exception {
        PathConsumer consumer = mock(PathConsumer.class);
        DB.execute(
                String.format("MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath.stream(start, end, '', { graph: '%s' }) " +
                        "YIELD nodeId, cost RETURN nodeId, cost", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    consumer.accept((Long) row.getNumber("nodeId"), (Double) row.getNumber("cost"));
                    return true;
                });
        verify(consumer, times(2)).accept(anyLong(), anyDouble());
        verify(consumer, times(1)).accept(anyLong(), eq(0.0));
        verify(consumer, times(1)).accept(anyLong(), eq(1.0));
    }

    @AllGraphNamesTest
    void noWeightWrite(String graphName) throws Exception {
        DB.execute(
                String.format("MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath(start, end, '', { graph: '%s' }) " +
                        "YIELD loadMillis, evalMillis, writeMillis, nodeCount, totalCost\n" +
                        "RETURN loadMillis, evalMillis, writeMillis, nodeCount, totalCost", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(1.0, (Double) row.getNumber("totalCost"), 0.01);
                    assertEquals(2L, row.getNumber("nodeCount"));
                    assertNotEquals(-1L, row.getNumber("loadMillis"));
                    assertNotEquals(-1L, row.getNumber("evalMillis"));
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    return false;
                });

        final CostConsumer mock = mock(CostConsumer.class);

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

    @AllGraphNamesTest
    void testDijkstraStream(String graphName) throws Exception {
        PathConsumer consumer = mock(PathConsumer.class);
        DB.execute(
                String.format("MATCH (start:Node{type:'start'}), (end:Node{type:'end'}) " +
                        "CALL algo.shortestPath.stream(start, end, 'cost',{graph:'%s'}) " +
                        "YIELD nodeId, cost RETURN nodeId, cost", graphName))
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

    @AllGraphNamesTest
    void testDijkstra(String graphName) throws Exception {
        DB.execute(
                String.format(
                        "MATCH (start:Node {type:'start'}), (end:Node {type:'end'}) " +
                        "CALL algo.shortestPath(start, end, 'cost',{graph:'%s', write:true, writeProperty:'cost'}) " +
                        "YIELD loadMillis, evalMillis, writeMillis, nodeCount, totalCost\n" +
                        "RETURN loadMillis, evalMillis, writeMillis, nodeCount, totalCost", graphName))
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(3.0, (Double) row.getNumber("totalCost"), 10E2);
                    assertEquals(4L, row.getNumber("nodeCount"));
                    assertNotEquals(-1L, row.getNumber("loadMillis"));
                    assertNotEquals(-1L, row.getNumber("evalMillis"));
                    assertNotEquals(-1L, row.getNumber("writeMillis"));
                    return false;
                });

        final CostConsumer mock = mock(CostConsumer.class);

        DB.execute("MATCH (n) WHERE exists(n.cost) RETURN id(n) as id, n.cost as cost")
                .accept(row -> {
                    mock.accept(
                            row.getNumber("id").longValue(),
                            row.getNumber("cost").doubleValue());
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

    interface CostConsumer {
        void accept(long nodeId, double cost);
    }
}
