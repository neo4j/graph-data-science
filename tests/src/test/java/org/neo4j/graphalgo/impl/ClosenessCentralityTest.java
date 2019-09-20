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
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TestSupport.AllGraphTypesWithoutCypherTest;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Graph:
 *
 *  (A)<-->(B)<-->(C)<-->(D)<-->(E)
 *
 * Calculation:
 *
 * N = 5        // number of nodes
 * k = N-1 = 4  // used for normalization
 *
 *      A     B     C     D     E
 *  --|-----------------------------
 *  A | 0     1     2     3     4       // farness between each pair of nodes
 *  B | 1     0     1     2     3
 *  C | 2     1     0     1     2
 *  D | 3     2     1     0     1
 *  E | 4     3     2     1     0
 *  --|-----------------------------
 *  S | 10    7     6     7     10      // sum each column
 *  ==|=============================
 * k/S| 0.4  0.57  0.67  0.57   0.4     // normalized centrality
 */
class ClosenessCentralityTest {

    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node {name: 'a'})" +
            ", (b:Node {name: 'b'})" +
            ", (c:Node {name: 'c'})" +
            ", (d:Node {name: 'd'})" +
            ", (e:Node {name: 'e'})" +
            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(a)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)" +
            ", (c)-[:TYPE]->(d)" +
            ", (d)-[:TYPE]->(c)" +
            ", (d)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(d)";

    private static final double[] EXPECTED = new double[]{0.4, 0.57, 0.66, 0.57, 0.4};
    private static GraphDatabaseAPI DB;

    @BeforeAll
    static void setupGraph() {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.execute(DB_CYPHER);
    }

    @AfterAll
    static void shutdown() {
        if (DB != null) DB.shutdown();
    }

    private Graph graph;

    @AllGraphTypesWithoutCypherTest
    void testGetCentrality(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final double[] centrality =
                new MSClosenessCentrality(
                        graph,
                        AllocationTracker.EMPTY,
                        Pools.DEFAULT_CONCURRENCY,
                        Pools.DEFAULT,
                        false)
                        .compute()
                        .exportToArray();

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }

    @AllGraphTypesWithoutCypherTest
    void testStream(Class<? extends GraphFactory> graphFactory) {
        setup(graphFactory);
        final double[] centrality = new double[(int) graph.nodeCount()];

        new MSClosenessCentrality(graph, AllocationTracker.EMPTY, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT, false)
                .compute()
                .resultStream()
                .forEach(r -> centrality[Math.toIntExact(graph.toMappedNodeId(r.nodeId))] = r.centrality);

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }

    private void setup(Class<? extends GraphFactory> graphImpl) {
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(graphImpl);
    }
}
