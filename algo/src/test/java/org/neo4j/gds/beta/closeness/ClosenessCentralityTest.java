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
package org.neo4j.gds.beta.closeness;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.beta.closeness.ClosenessCentrality.centrality;

/**
 * Graph:
 *
 * (A)<-->(B)<-->(C)<-->(D)<-->(E)
 *
 * Calculation:
 *
 * N = 5        // number of nodes
 * k = N-1 = 4  // used for normalization
 *
 * A     B     C     D     E
 * --|-----------------------------
 * A | 0     1     2     3     4       // farness between each pair of nodes
 * B | 1     0     1     2     3
 * C | 2     1     0     1     2
 * D | 3     2     1     0     1
 * E | 4     3     2     1     0
 * --|-----------------------------
 * S | 10    7     6     7     10      // sum each column
 * ==|=============================
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

    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testGetCentrality() {
        var algo = new ClosenessCentrality(
            graph,
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
            false,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = algo.compute().getCentrality();

        assertThat(result.get(idFunction.of("a"))).isCloseTo(0.4, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("b"))).isCloseTo(0.57, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("c"))).isCloseTo(0.66, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("d"))).isCloseTo(0.57, Offset.offset(0.01));
        assertThat(result.get(idFunction.of("e"))).isCloseTo(0.4, Offset.offset(0.01));
    }

    @Test
    void testCentralityFormula() {
        /*
            C(u) = \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)}

            C_{WF}(u) = \frac{n-1}{N-1} \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)}

            where `d(v, u)` is the shortest-path distance between `v` and `u`
                  `n` is the number of nodes that can reach `u`
                  `N` is the number of nodes in the graph
         */

        assertEquals(1.0, centrality(5, 5, 10, false), 0.01);
        assertEquals(0.5, centrality(10, 5, 10, false), 0.01);
        assertEquals(0, centrality(0, 0, 10, false), 0.01);

        assertEquals(5 / 9D, centrality(5, 5, 10, true), 0.01);
        assertEquals(1.25, centrality(5, 5, 5, true), 0.01);
    }
}
