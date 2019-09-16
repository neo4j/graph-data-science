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
package org.neo4j.graphalgo.impl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.AdditionalMatchers;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.closeness.HarmonicCentrality;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Disconnected-Graph:
 *
 *  (A)<-->(B)<-->(C)  (D)<-->(E)
 *
 * Calculation:
 *
 * k = N-1 = 4
 *
 *      A     B     C     D     E
 *  --|-----------------------------
 *  A | 0     1     2     -     -    // distance between each pair of nodes
 *  B | 1     0     1     -     -    // or infinite of no path exists
 *  C | 2     1     0     -     -
 *  D | -     -     -     0     1
 *  E | -     -     -     1     0
 *  --|------------------------------
 * -1 |1.5    2    1.5    1     1
 *  ==|==============================
 * *k |0.37  0.5  0.37  0.25  0.25
 *
 * instead of calculating the farness we sum the inverse
 * of each cell and multiply by 1/(n-1)
 */
class HarmonicCentralityTest {

    private static final String CYPHER = "CREATE " +
                                         "  (a:Node {name:'a'})" +
                                         ", (b:Node {name:'b'})" +
                                         ", (c:Node {name:'c'})" +
                                         ", (d:Node {name:'d'})" +
                                         ", (e:Node {name:'e'})" +
                                         ", (a)-[:TYPE]->(b)" +
                                         ", (b)-[:TYPE]->(c)" +
                                         ", (d)-[:TYPE]->(e)";

    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraph() {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(CYPHER);
    }

    @AfterAll
    static void shutdown() {
        if (DB != null) DB.shutdown();
    }

    @AllGraphTypesWithoutCypherTest
    void testStream(Class<? extends GraphFactory> graphImpl) {
        Graph graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(graphImpl);

        final Consumer mock = mock(Consumer.class);

        new HarmonicCentrality(graph, AllocationTracker.EMPTY, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT)
                .compute()
                .resultStream()
                .forEach(r -> mock.consume(r.nodeId, r.centrality));

        verify(mock, times(2)).consume(anyLong(), AdditionalMatchers.eq(0.375, 0.1));
        verify(mock, times(2)).consume(anyLong(), AdditionalMatchers.eq(0.25, 0.1));
        verify(mock, times(1)).consume(anyLong(), AdditionalMatchers.eq(0.5, 0.1));
    }

    interface Consumer {
        void consume(long id, double centrality);
    }
}
