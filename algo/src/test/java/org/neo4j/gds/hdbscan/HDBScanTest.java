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
package org.neo4j.gds.hdbscan;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class HDBScanTest {

    @GdlGraph
    private static final String DATA =
        """
            CREATE
                (a:Node {point: [1.0, 1.0]}),
                (b:Node {point: [1.0, 5.0]}),
                (c:Node {point: [1.0, 6.0]}),
                (d:Node {point: [2.0, 2.0]}),
                (e:Node {point: [8.0, 2.0]}),
                (f:Node {point: [10.0, 1.0]})
                (g:Node {point: [10.0, 2.0]})
                (h:Node {point: [12.0, 3.0]})
                (i:Node {point: [12.0, 21.0]})
            """;

    @Inject
    private TestGraph graph;

    @Test
    void shouldComputeCoresCorrectly(){

        var hdbscan =new HDBScan(graph,graph.nodeProperties("point"),
            new Concurrency(1),
            1,
            2,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        var kdtree = hdbscan.buildKDTree();

        var cores = hdbscan.computeCores(kdtree,graph.nodeCount()).createCoreArray();

        assertThat(cores.toArray()).containsExactlyInAnyOrder(
            4.0,        //a -  d,b  (sqrt(16)
            Math.sqrt(10), //b -  c,d  (sqrt(1 + 9)=sqrt(10)
            Math.sqrt(17),  //c -  b,d  (sqrt(1 + 16) = sqrt(17)
            Math.sqrt(10), //d -  a,b
            Math.sqrt(5),   //e - g,f (sqrt(1 + 4) = sqrt(5)
            Math.sqrt(5), //f - g,e
            2.0,   //g - f,e (sqrt(4)
            2.0 *Math.sqrt(2),  //h - g,f (sqrt( 4 + 4) = sqrt(8) = 2sqrt(2)
           Math.sqrt(346) //i - h, c (sqrt(11^2 + 15^2) = sqrt(346)
        );

    }
}
