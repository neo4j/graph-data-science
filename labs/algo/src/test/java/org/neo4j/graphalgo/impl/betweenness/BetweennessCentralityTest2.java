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
package org.neo4j.graphalgo.impl.betweenness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * .0                 .0
 * (a)                 (f)
 * | \               / |
 * |  \8.0  9.0  8.0/  |
 * |  (c)---(d)---(e)  |
 * |  /            \   |
 * | /              \  |
 * (b)                (g)
 * .0                 .0
 */
@ExtendWith(MockitoExtension.class)
class BetweennessCentralityTest2 extends AlgoTestBase {

    private static final String CYPHER =
        "CREATE" +
        "  (a:Node {name: 'a'})" +
        ", (b:Node {name: 'b'})" +
        ", (c:Node {name: 'c'})" +
        ", (d:Node {name: 'd'})" +
        ", (e:Node {name: 'e'})" +
        ", (f:Node {name: 'f'})" +
        ", (g:Node {name: 'g'})" +

        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(c)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (e)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";

    private static Graph graph;

    interface TestConsumer {
        void accept(String name, double centrality);
    }

    @Mock
    private TestConsumer testConsumer;

    @BeforeEach
    void setupGraphDb() {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(CYPHER);
    }

    @AfterEach
    void shutdownGraphDb() {
        db.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testBC(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);

        BetweennessCentrality algo = new BetweennessCentrality(graph);
        algo.compute();
        algo.resultStream()
            .forEach(r -> testConsumer.accept(name(r.nodeId), r.centrality));

        verifyMock(testConsumer);
    }

    @AllGraphTypesWithoutCypherTest
    void testPBC(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);

        ParallelBetweennessCentrality algo = new ParallelBetweennessCentrality(
            graph,
            Pools.DEFAULT,
            4
        );
        algo.compute();
        algo.resultStream()
            .forEach(r -> testConsumer.accept(name(r.nodeId), r.centrality));

        verifyMock(testConsumer);
    }

    void verifyMock(TestConsumer mock) {
        verify(mock, times(1)).accept(eq("a"), eq(0.0));
        verify(mock, times(1)).accept(eq("b"), eq(0.0));
        verify(mock, times(1)).accept(eq("c"), eq(8.0));
        verify(mock, times(1)).accept(eq("d"), eq(9.0));
        verify(mock, times(1)).accept(eq("e"), eq(8.0));
        verify(mock, times(1)).accept(eq("f"), eq(0.0));
        verify(mock, times(1)).accept(eq("g"), eq(0.0));
    }


    @AllGraphTypesWithoutCypherTest
    void testIterateParallel(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);

        final AtomicIntegerArray ai = new AtomicIntegerArray(1001);

        ParallelUtil.iterateParallel(Pools.DEFAULT, 1001, 8, i -> {
            ai.set(i, i);
        });

        for (int i = 0; i < 1001; i++) {
            assertEquals(i, ai.get(i));
        }
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(db)
            .withAnyRelationshipType()
            .withAnyLabel()
            .load(graphImpl);
    }

    private String name(long id) {
        String[] name = {""};
        runQuery(
            "MATCH (n:Node) WHERE id(n) = " + id + " RETURN n.name as name",
            row -> name[0] = row.getString("name")
        );
        return name[0];
    }
}
