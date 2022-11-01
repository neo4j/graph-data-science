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
package org.neo4j.gds.leiden;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.modularity.TestGraphs;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LocalMovePhaseKarateTest {

    @SuppressFBWarnings("HSC_HUGE_SHARED_STRING_CONSTANT")
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER = TestGraphs.KARATE_CLUB_GRAPH;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testLocalMovePhase() {
        var localMoveCommunities = LeidenUtils.createSingleNodeCommunities(graph.nodeCount());

        var nodeVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        nodeVolumes.setAll(graph::degree);

        var communityVolumes = nodeVolumes.copyOf(graph.nodeCount());

        double gamma = 1.0 / graph.relationshipCount();

        LocalMovePhase.create(
            graph,
            localMoveCommunities,
            nodeVolumes,
            communityVolumes,
            gamma,
            graph.nodeCount(), 1
        ).run();


        var communitiesMap = LongStream
            .range(0, localMoveCommunities.size())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> localMoveCommunities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("a0"),
                community -> assertThat(community).containsExactlyInAnyOrder("a5", "a11"),
                community -> assertThat(community).containsExactlyInAnyOrder("a3", "a4", "a8", "a10", "a13", "a14"),
                community -> assertThat(community).containsExactlyInAnyOrder("a6", "a7", "a17"),
                community -> assertThat(community).containsExactlyInAnyOrder("a1", "a2", "a12", "a18", "a20", "a22"),
                community -> assertThat(community).containsExactlyInAnyOrder("a24", "a25", "a26", "a28", "a29", "a32"),
                community -> assertThat(community).containsExactlyInAnyOrder("a9",
                    "a15",
                    "a16",
                    "a19",
                    "a21",
                    "a23",
                    "a27",
                    "a30",
                    "a31",
                    "a33",
                    "a34"
                )
            );
    }

}
