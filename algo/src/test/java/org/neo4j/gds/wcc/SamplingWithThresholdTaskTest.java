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
package org.neo4j.gds.wcc;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.CommunityHelper;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.gds.Orientation.NATURAL;

@GdlExtension
class SamplingWithThresholdTaskTest {

    @GdlGraph(orientation = NATURAL)
    static String GDL =
        "  (a)-[:REL { p: 1.0 } ]->(b)" +
        ", (a)-[:REL { p: 0.5 } ]->(c)" +
        ", (a)-[:REL { p: 1.0 } ]->(d)" +
        ", (d)-[:REL { p: 1.0 } ]->(e)";

    @Inject
    private TestGraph graph;

    @Test
    void shouldOnlySampleTheFirstTwoElements() {
        var components = new HugeAtomicDisjointSetStruct(graph.nodeCount(), 2);
        var partition = Partition.of(0, graph.nodeCount());

        var task = new SampledStrategy.SamplingWithThresholdTask(
            graph,
            0.5,
            partition,
            components,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        task.run();

        var actualCommunities = new ArrayList<Long>();
        graph.forEachNode(node -> actualCommunities.add(components.setIdOf(node)));
        CommunityHelper.assertCommunities(
            actualCommunities,
            List.of(
                // (a)-[:REL { p: 1.0 } ]->(b) => union
                // (a)-[:REL { p: 0.5 } ]->(c) => skipped due to threshold = 0.5
                // (a)-[:REL { p: 1.0 } ]->(d) => union
                // (d)-[:REL { p: 1.0 } ]->(e) => union
                List.of(graph.toMappedNodeId("a"), graph.toMappedNodeId("b"), graph.toMappedNodeId("d"), graph.toMappedNodeId("e")),
                List.of(graph.toMappedNodeId("c"))
            )
        );
    }
}
