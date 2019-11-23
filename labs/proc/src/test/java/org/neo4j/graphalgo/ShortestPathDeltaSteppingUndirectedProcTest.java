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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphalgo.TestSupport.AllGraphNamesTest;

import java.util.function.DoubleConsumer;

import static org.mockito.AdditionalMatchers.eq;
import static org.mockito.ArgumentMatchers.anyDouble;
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
final class ShortestPathDeltaSteppingUndirectedProcTest extends ProcTestBase {

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

            ", (s)-[:TYPE {cost :5}]->(a)" + // line 1
            ", (a)-[:TYPE {cost :5}]->(b)" +
            ", (b)-[:TYPE {cost :5}]->(c)" +
            ", (c)-[:TYPE {cost :5}]->(x)" +

            ", (s)-[:TYPE {cost :3}]->(d)" + // line 2
            ", (d)-[:TYPE {cost :3}]->(e)" +
            ", (e)-[:TYPE {cost :3}]->(f)" +
            ", (f)-[:TYPE {cost :3}]->(x)" +

            ", (s)-[:TYPE {cost :2}]->(g)" + // line 3
            ", (g)-[:TYPE {cost :2}]->(h)" +
            ", (h)-[:TYPE {cost :2}]->(i)" +
            ", (i)-[:TYPE {cost :2}]->(x)";

    @BeforeEach
    void setup() throws RegistrationException {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(ShortestPathDeltaSteppingProc.class);
        db.execute(DB_CYPHER);
    }

    @AfterEach
    void shutdownGraph() {
        db.shutdown();
    }

    @AllGraphNamesTest
    void testOutgoingResultStream(String graphName) {

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String cypher = "MATCH(n:Node {name:'x'}) " +
                              "WITH n CALL algo.shortestPath.deltaStepping.stream(n, 'cost', 3.0,{graph:'" + graphName + "', direction: 'OUTGOING'}) " +
                              "YIELD nodeId, distance RETURN nodeId, distance";

        db.execute(cypher).accept(row -> {
            double distance = row.getNumber("distance").doubleValue();
            consumer.accept(distance);
            return true;
        });

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(10)).accept(eq(Double.POSITIVE_INFINITY, 0.1D));
    }

    @AllGraphNamesTest
    void testUndirectedResultStream(String graphName) {

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String cypher = "MATCH(n:Node {name:'x'}) " +
                              "WITH n CALL algo.shortestPath.deltaStepping.stream(n, 'cost', 3.0,{graph:'" + graphName + "'}) " +
                              "YIELD nodeId, distance RETURN nodeId, distance";

        db.execute(cypher).accept(row -> {
            double distance = row.getNumber("distance").doubleValue();
            consumer.accept(distance);
            return true;
        });

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(1)).accept(eq(8, 0.1D));
    }
}
