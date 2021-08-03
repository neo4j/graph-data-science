/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.impl;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.impl.closeness.MSClosenessCentrality;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.ConcurrencyConfig;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

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
@GdlExtension
class ClosenessCentralityTest {

    @GdlGraph
    private static final String DB_CYPHER =
            "CREATE " +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +

            ", (a)-[:TYPE]->(b)" +
            ", (b)-[:TYPE]->(a)" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)" +
            ", (c)-[:TYPE]->(d)" +
            ", (d)-[:TYPE]->(c)" +
            ", (d)-[:TYPE]->(e)" +
            ", (e)-[:TYPE]->(d)";

    private static final double[] EXPECTED = new double[]{0.4, 0.57, 0.66, 0.57, 0.4};

    @Inject
    private Graph graph;

    @Test
    void testGetCentrality() {
        MSClosenessCentrality algo = new MSClosenessCentrality(
            graph,
            AllocationTracker.empty(),
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
            Pools.DEFAULT,
            false
        );
        algo.compute();
        final double[] centrality = algo.exportToArray();

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }

    @Test
    void testStream() {
        final double[] centrality = new double[(int) graph.nodeCount()];

        MSClosenessCentrality algo = new MSClosenessCentrality(
            graph,
            AllocationTracker.empty(),
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
            Pools.DEFAULT,
            false
        );
        algo.compute();
        algo.resultStream()
            .forEach(r -> centrality[Math.toIntExact(graph.toMappedNodeId(r.nodeId))] = r.centrality);

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }
}
