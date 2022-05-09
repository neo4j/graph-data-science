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
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class RefinementPhaseKarateTest {


    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "(a0: Node)," +
        "(a1: Node)," +
        "(a2: Node)," +
        "(a3: Node)," +
        "(a4: Node)," +
        "(a5: Node)," +
        "(a6: Node)," +
        "(a7: Node)," +
        "(a8: Node)," +
        "(a9: Node)," +
        "(a10: Node)," +
        "(a11: Node)," +
        "(a12: Node)," +
        "(a13: Node)," +
        "(a14: Node)," +
        "(a15: Node)," +
        "(a16: Node)," +
        "(a17: Node)," +
        "(a18: Node)," +
        "(a19: Node)," +
        "(a20: Node)," +
        "(a21: Node)," +
        "(a22: Node)," +
        "(a23: Node)," +
        "(a24: Node)," +
        "(a25: Node)," +
        "(a26: Node)," +
        "(a27: Node)," +
        "(a28: Node)," +
        "(a29: Node)," +
        "(a30: Node)," +
        "(a31: Node)," +
        "(a32: Node)," +
        "(a33: Node)," +
        "(a34: Node)," +
        "(a1)-[:R]->(a2)," +
        "(a1)-[:R]->(a3)," +
        "(a2)-[:R]->(a3)," +
        "(a1)-[:R]->(a4)," +
        "(a2)-[:R]->(a4)," +
        "(a3)-[:R]->(a4)," +
        "(a1)-[:R]->(a5)," +
        "(a1)-[:R]->(a6)," +
        "(a1)-[:R]->(a7)," +
        "(a5)-[:R]->(a7)," +
        "(a6)-[:R]->(a7)," +
        "(a1)-[:R]->(a8)," +
        "(a2)-[:R]->(a8)," +
        "(a3)-[:R]->(a8)," +
        "(a4)-[:R]->(a8)," +
        "(a1)-[:R]->(a9)," +
        "(a3)-[:R]->(a9)," +
        "(a3)-[:R]->(a10)," +
        "(a1)-[:R]->(a11)," +
        "(a5)-[:R]->(a11)," +
        "(a6)-[:R]->(a11)," +
        "(a1)-[:R]->(a12)," +
        "(a1)-[:R]->(a13)," +
        "(a4)-[:R]->(a13)," +
        "(a1)-[:R]->(a14)," +
        "(a2)-[:R]->(a14)," +
        "(a3)-[:R]->(a14)," +
        "(a4)-[:R]->(a14)," +
        "(a6)-[:R]->(a17)," +
        "(a7)-[:R]->(a17)," +
        "(a1)-[:R]->(a18)," +
        "(a2)-[:R]->(a18)," +
        "(a1)-[:R]->(a20)," +
        "(a2)-[:R]->(a20)," +
        "(a1)-[:R]->(a22)," +
        "(a2)-[:R]->(a22)," +
        "(a24)-[:R]->(a26)," +
        "(a25)-[:R]->(a26)," +
        "(a3)-[:R]->(a28) ," +
        "(a24)-[:R]->(a28)," +
        "(a25)-[:R]->(a28)," +
        "(a3)-[:R]->(a29) ," +
        "(a24)-[:R]->(a30)," +
        "(a27)-[:R]->(a30)," +
        "(a2)-[:R]->(a31) ," +
        "(a9)-[:R]->(a31) ," +
        "(a1)-[:R]->(a32) ," +
        "(a25)-[:R]->(a32)," +
        "(a26)-[:R]->(a32)," +
        "(a29)-[:R]->(a32)," +
        "(a3)-[:R]->(a33) ," +
        "(a9)-[:R]->(a33) ," +
        "(a15)-[:R]->(a33)," +
        "(a16)-[:R]->(a33)," +
        "(a19)-[:R]->(a33)," +
        "(a21)-[:R]->(a33)," +
        "(a23)-[:R]->(a33)," +
        "(a24)-[:R]->(a33)," +
        "(a30)-[:R]->(a33)," +
        "(a31)-[:R]->(a33)," +
        "(a32)-[:R]->(a33)," +
        "(a9)-[:R]->(a34) ," +
        "(a10)-[:R]->(a34)," +
        "(a14)-[:R]->(a34)," +
        "(a15)-[:R]->(a34)," +
        "(a16)-[:R]->(a34)," +
        "(a19)-[:R]->(a34)," +
        "(a20)-[:R]->(a34)," +
        "(a21)-[:R]->(a34)," +
        "(a23)-[:R]->(a34)," +
        "(a24)-[:R]->(a34)," +
        "(a27)-[:R]->(a34)," +
        "(a28)-[:R]->(a34)," +
        "(a29)-[:R]->(a34)," +
        "(a30)-[:R]->(a34)," +
        "(a31)-[:R]->(a34)," +
        "(a32)-[:R]->(a34)," +
        "(a33)-[:R]->(a34)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testRefinementPhase() {
        var seed = HugeLongArray.newArray(graph.nodeCount());
        seed.setAll(nodeId -> nodeId);

        var nodeVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        nodeVolumes.setAll(graph::degree);

        var communityVolumes = nodeVolumes.copyOf(graph.nodeCount());

        double gamma = 1.0 / graph.relationshipCount();

        var localMovePhase = LocalMovePhase.create(graph, seed, nodeVolumes, communityVolumes, gamma);

        var localMovePhaseResult = localMovePhase.run();
        var originalCommunities = localMovePhaseResult.communities();
        var communityVolumesForRefinement = localMovePhaseResult.communityVolumes();

        var refinementPhase = new RefinementPhase(
            graph,
            originalCommunities,
            nodeVolumes,
            communityVolumesForRefinement,
            gamma,
            0.01,
            19L
        );

        var refinementPhaseResult = refinementPhase.run();
        var refinedCommunities = refinementPhaseResult.communities();

        var communitiesMap = LongStream
            .range(0, refinedCommunities.size())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> refinedCommunities.get(idFunction.of(v))));

        communitiesMap.forEach((c, members) -> System.out.println(c + ": " + String.join(", ", members)));

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
