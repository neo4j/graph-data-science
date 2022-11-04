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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
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
class RefinementPhaseKarateTest {


    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER = TestGraphs.KARATE_CLUB_GRAPH;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testRefinementPhase() {
        var originalCommunities = LeidenUtils.createSingleNodeCommunities(graph.nodeCount());

        var nodeVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        nodeVolumes.setAll(graph::degree);

        var communityVolumes = nodeVolumes.copyOf(graph.nodeCount());

        double gamma = 1.0 / graph.relationshipCount();

        var localMovePhase = LocalMovePhase.create(graph,
            originalCommunities, nodeVolumes, communityVolumes, gamma, 1
        );

        localMovePhase.run();
        var communityVolumesForRefinement = communityVolumes;

        var refinementPhase = RefinementPhase.create(
            graph,
            originalCommunities,
            nodeVolumes,
            communityVolumesForRefinement,
            gamma,
            0.01,
            19L,
            1,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var refinementPhaseResult = refinementPhase.run();
        var refinedCommunities = refinementPhaseResult.communities();

        var communitiesMap = LongStream
            .range(0, refinedCommunities.size())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> refinedCommunities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("a0"),
                community -> assertThat(community).containsExactlyInAnyOrder("a5", "a11"),
                community -> assertThat(community).containsExactlyInAnyOrder("a3", "a10", "a14"),
                community -> assertThat(community).containsExactlyInAnyOrder("a4", "a8", "a13"),
                community -> assertThat(community).containsExactlyInAnyOrder("a6", "a7", "a17"),
                community -> assertThat(community).containsExactlyInAnyOrder("a1", "a12"),
                community -> assertThat(community).containsExactlyInAnyOrder("a2", "a18", "a20", "a22"),
                community -> assertThat(community).containsExactlyInAnyOrder("a24", "a26"),
                community -> assertThat(community).containsExactlyInAnyOrder("a25", "a28"),
                community -> assertThat(community).containsExactlyInAnyOrder("a29", "a32"),
                community -> assertThat(community).containsExactlyInAnyOrder("a27", "a30"),
                community -> assertThat(community).containsExactlyInAnyOrder("a9", "a31"),
                community -> assertThat(community).containsExactlyInAnyOrder("a15", "a16", "a19", "a23", "a33"),
                community -> assertThat(community).containsExactlyInAnyOrder("a21", "a34")
            );
    }
}
