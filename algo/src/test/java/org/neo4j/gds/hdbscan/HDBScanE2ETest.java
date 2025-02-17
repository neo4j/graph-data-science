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
class HDBScanE2ETest {

    @GdlGraph
    private static final String DATA =
        """
        CREATE
            (a:Node {point: [1.17755754, 2.02742572]}),
            (b:Node {point: [0.88489682, 1.97328227]}),
            (c:Node {point: [1.04192267, 4.34997048]}),
            (d:Node {point: [1.25764886, 1.94667762]}),
            (e:Node {point: [0.95464318, 1.55300632]}),
            (f:Node {point: [0.80617459, 1.60491802]}),
            (g:Node {point: [1.26227786, 3.96066446]}),
            (h:Node {point: [0.87569985, 4.51938412]}),
            (i:Node {point: [0.8028515 , 4.088106  ]}),
            (j:Node {point: [0.82954022, 4.63897487]})
        """;

    @Inject
    private TestGraph graph;

    @Test
    void hdbscan() {
        var hdbScan = new HDBScan(
            graph,
            graph.nodeProperties("point"),
            new Concurrency(1),
            1,
            2,
            2,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var labelsWithOffset = hdbScan.compute();

        var labels = new long[10];
        for (char letter='a'; letter<='j';++letter){
            var offsetPosition = graph.toMappedNodeId(String.valueOf(letter));
            labels[letter-'a'] = labelsWithOffset.get(offsetPosition);
        }

        var expectedLabels = new long[] {2, 2, 1, 2, 2, 2, 1, 1, 1, 1};

        assertThat(labels).containsExactly(expectedLabels);
    }


}
