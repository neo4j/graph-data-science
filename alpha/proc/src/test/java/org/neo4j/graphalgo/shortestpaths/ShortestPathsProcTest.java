/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.shortestpaths;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Matchers;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.QueryRunner;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.newapi.GraphCreateProc;
import org.neo4j.graphdb.Label;

import java.util.function.DoubleConsumer;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
final class ShortestPathsProcTest extends BaseProcTest {

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

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(ShortestPathsProc.class, GraphCreateProc.class);
        runQuery(DB_CYPHER);
        QueryRunner.runInTransaction(db, () -> {
            startNode = db.findNode(Label.label("Node"), "name", "s").getId();
            endNode = db.findNode(Label.label("Node"), "name", "x").getId();
        });

        String graphCreateQuery = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType(
                "TYPE",
                RelationshipProjection.of(
                    "TYPE",
                    Projection.UNDIRECTED,
                    Aggregation.DEFAULT
                )
            )
            .withRelationshipProperty(PropertyMapping.of("cost", 1.0d))
            .graphCreate("shortestPathsGraph")
            .yields();

        runQuery(graphCreateQuery);
    }

    @AfterEach
    void shutdownGraph() {
        GraphCatalog.removeAllLoadedGraphs();
        db.shutdown();
    }

    @Test
    void testResultStream() {

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String cypher = "MATCH(n:Node {name:'s'}) " +
                              "WITH n CALL gds.alpha.shortestPaths.stream('shortestPathsGraph'," +
                              "{" +
                              "     startNode: n," +
                              "     relationshipWeightProperty: 'cost'" +
                              "}) " +
                              "YIELD nodeId, distance RETURN nodeId, distance";

        runQueryWithRowConsumer(cypher, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double distance = row.getNumber("distance").doubleValue();
            consumer.accept(distance);
            System.out.printf(
                "%d:%.1f, ",
                nodeId,
                distance
            );
        });

        System.out.println();

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(1)).accept(eq(8D));
    }

    @Test
    void testWriteBack() {
        final String matchCypher = "MATCH(n:Node {name:'s'}) " +
                                   "WITH n CALL gds.alpha.shortestPaths.write(" +
                                   "        'shortestPathsGraph', " +
                                   "        {" +
                                   "            startNode: n, " +
                                   "            relationshipWeightProperty: 'cost', " +
                                   "            writeProperty: 'sp'" +
                                   "        })" +
                                   " YIELD nodeCount, loadMillis, evalMillis, writeMillis" +
                                   " RETURN nodeCount, loadMillis, evalMillis, writeMillis";

        runQueryWithRowConsumer(matchCypher, row -> {
            System.out.println("loadMillis = " + row.getNumber("loadMillis").longValue());
            System.out.println("evalMillis = " + row.getNumber("evalMillis").longValue());
            long writeMillis = row.getNumber("writeMillis").longValue();
            System.out.println("writeMillis = " + writeMillis);
            System.out.println("nodeCount = " + row.getNumber("nodeCount").longValue());
            assertNotEquals(-1L, writeMillis);
        });

        final DoubleConsumer consumer = mock(DoubleConsumer.class);

        final String testCypher = "MATCH(n:Node) WHERE exists(n.sp) WITH n RETURN id(n) as id, n.sp as sp";

        runQueryWithRowConsumer(testCypher, row -> {
            double sp = row.getNumber("sp").doubleValue();
            consumer.accept(sp);
        });

        verify(consumer, times(11)).accept(anyDouble());
        verify(consumer, times(1)).accept(eq(8D));
    }

    @Test
    void testData() {

        final Consumer mock = mock(Consumer.class);

        final String cypher = "MATCH(n:Node {name:'x'}) " +
                              "WITH n CALL gds.alpha.shortestPaths.stream('shortestPathsGraph', {startNode: n, relationshipWeightProperty: 'cost'}) " +
                              "YIELD nodeId, distance RETURN nodeId, distance";

        runQueryWithRowConsumer(cypher, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double distance = row.getNumber("distance").doubleValue();
            System.out.printf(
                "%d:%.1f, ",
                nodeId,
                distance
            );
            mock.test(nodeId, distance);
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
