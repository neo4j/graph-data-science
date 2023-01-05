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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.dss.HugeAtomicDisjointSetStruct;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.gds.Orientation.NATURAL;

@GdlExtension
class LinkTaskTest {

    @GdlGraph(orientation = NATURAL)
    static String GDL =
        "  (a)-->(b)" +
        ", (a)-->(c)" +
        ", (a)-->(d)" +
        ", (d)-->(b)" +
        ", (d)-->(c)" +
        ", (d)-->(e)";


    @Inject
    private Graph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldNotUnionNodesInSkipComponent() {
        var components = new HugeAtomicDisjointSetStruct(graph.nodeCount(), 2);
        var partition = Partition.of(0, graph.nodeCount());

        var task = new SampledStrategy.LinkTask(
            graph,
            partition,
            idFunction.of("a"),
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
                // (a)-->(b) => skipped due to skipComponent = a
                // (a)-->(c) => skipped due to skipComponent = a
                // (a)-->(d) => skipped due to skipComponent = a
                // (d)-->(b) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-->(c) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-->(e) => union
                List.of(idFunction.of("a")),
                List.of(idFunction.of("b")),
                List.of(idFunction.of("c")),
                List.of(idFunction.of("d"), idFunction.of("e"))
            )
        );
    }

    @Test
    void shouldSkipTheFirstTwoElements() {
        var components = new HugeAtomicDisjointSetStruct(graph.nodeCount(), 2);
        var partition = Partition.of(0, graph.nodeCount());

        var task = new SampledStrategy.LinkTask(
            graph,
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
                // (a)-->(b) => skipped due to NEIGHBOR_ROUNDS = 2
                // (a)-->(c) => skipped due to NEIGHBOR_ROUNDS = 2
                // (a)-->(d) => union
                // (d)-->(b) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-->(c) => skipped due to NEIGHBOR_ROUNDS = 2
                // (d)-->(e) => union
                List.of(idFunction.of("a"), idFunction.of("d"), idFunction.of("e")),
                List.of(idFunction.of("b")),
                List.of(idFunction.of("c"))
            )
        );
    }
}
