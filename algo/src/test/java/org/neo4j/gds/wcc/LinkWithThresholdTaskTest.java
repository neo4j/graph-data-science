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
class LinkWithThresholdTaskTest {

    @GdlGraph(orientation = NATURAL)
    static String GDL =
        "  (a)-[:REL { p: 1.0 } ]->(b)" +
        ", (a)-[:REL { p: 1.0 } ]->(c)" +
        ", (a)-[:REL { p: 1.0 } ]->(d)" +
        ", (d)-[:REL { p: 1.0 } ]->(b)" +
        ", (d)-[:REL { p: 1.0 } ]->(c)" +
        ", (d)-[:REL { p: 0.5 } ]->(e)" +
        ", (d)-[:REL { p: 1.0 } ]->(f)";

    @Inject
    private TestGraph graph;

    @Test
    void shouldNotUnionNodesInSkipComponent() {
        var components = new HugeAtomicDisjointSetStruct(graph.nodeCount(), 2);
        var partition = Partition.of(0, graph.nodeCount());

        var task = new SampledStrategy.LinkWithThresholdTask(
            graph,
            0.5,
            partition,
            graph.toMappedNodeId("a"),
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
                // (a)-[:REL { p: 1.0 } ]->(b) => skipped due to skipComponent = a
                // (a)-[:REL { p: 1.0 } ]->(c) => skipped due to skipComponent = a
                // (a)-[:REL { p: 1.0 } ]->(d) => skipped due to skipComponent = a
                // (d)-[:REL { p: 1.0 } ]->(b) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-[:REL { p: 1.0 } ]->(c) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-[:REL { p: 0.5 } ]->(e) => skipped due to threshold = 0.5
                // (d)-[:REL { p: 1.0 } ]->(f) => union
                List.of(graph.toMappedNodeId("a")),
                List.of(graph.toMappedNodeId("b")),
                List.of(graph.toMappedNodeId("c")),
                List.of(graph.toMappedNodeId("e")),
                List.of(graph.toMappedNodeId("d"), graph.toMappedNodeId("f"))
            )
        );
    }

    @Test
    void shouldSkipTheFirstTwoElements() {
        var components = new HugeAtomicDisjointSetStruct(graph.nodeCount(), 2);
        var partition = Partition.of(0, graph.nodeCount());

        var task = new SampledStrategy.LinkWithThresholdTask(
            graph,
            0.5,
            partition,
            -1,
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
                // (a)-[:REL { p: 1.0 } ]->(b) => skipped due to NEIGHBOR_ROUNDS = 2
                // (a)-[:REL { p: 1.0 } ]->(c) => skipped due to NEIGHBOR_ROUNDS = 2
                // (a)-[:REL { p: 1.0 } ]->(d) => union
                // (d)-[:REL { p: 1.0 } ]->(b) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-[:REL { p: 1.0 } ]->(c) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-[:REL { p: 0.5 } ]->(e) => skipped due to threshold = 0.5
                // (d)-[:REL { p: 1.0 } ]->(f) => union
                List.of(graph.toMappedNodeId("a"), graph.toMappedNodeId("d"), graph.toMappedNodeId("f")),
                List.of(graph.toMappedNodeId("b")),
                List.of(graph.toMappedNodeId("c")),
                List.of(graph.toMappedNodeId("e"))
            )
        );
    }
}
