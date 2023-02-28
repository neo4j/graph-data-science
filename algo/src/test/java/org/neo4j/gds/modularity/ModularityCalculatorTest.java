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
package org.neo4j.gds.modularity;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ModularityCalculatorTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static final String GRAPH =
        "CREATE " +
        " (a1: Node { communityId: 0, communityId2: 200 })," +
        " (a2: Node { communityId: 0, communityId2: 200 })," +
        " (a3: Node { communityId: 5, communityId2: 205 })," +
        " (a4: Node { communityId: 0, communityId2: 200 })," +
        " (a5: Node { communityId: 5, communityId2: 205 })," +
        " (a6: Node { communityId: 5, communityId2: 205 })," +

        " (a1)-[:R]->(a2)," +
        " (a1)-[:R]->(a4)," +
        " (a2)-[:R]->(a3)," +
        " (a2)-[:R]->(a4)," +
        " (a2)-[:R]->(a5)," +
        " (a3)-[:R]->(a6)," +
        " (a4)-[:R]->(a5)," +
        " (a5)-[:R]->(a6)";

    @Inject
    private TestGraph graph;
    @Inject
    private GraphStore graphStore;

    @ParameterizedTest
    @CsvSource({"communityId,0", "communityId2,200"})
    void compute(String communityId, int startingId) {
        var modularityCalculator = ModularityCalculator.create(
            graph,
            graphStore.nodeProperty(communityId).values()::longValue,
            4
        );

        var result = modularityCalculator.compute();
        var community_0_score = (6 - 9 * 9 * (1.0 / 16)) / 16;
        var community_5_score = (4 - 7 * 7 * (1.0 / 16)) / 16;
        assertThat(result.totalModularity()).isEqualTo(community_0_score + community_5_score);
        assertThat(result.communityCount()).isEqualTo(2L);

        var modularities = result.modularityScores().toArray();

        assertThat(modularities)
            .containsExactlyInAnyOrder(
                CommunityModularity.of(startingId, community_0_score),
                CommunityModularity.of(startingId + 5L, community_5_score)
            );
    }

}
