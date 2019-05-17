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
package org.neo4j.graphalgo.impl.closeness;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality.centrality;

public class MSBFSCCAlgorithmTest {
    @Test
    public void testCentralityFormula() {
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

        assertEquals(0.44444, centrality(5, 5, 10, true), 0.01);
        assertEquals(1.0, centrality(5, 5, 5, true), 0.01);
    }
}
