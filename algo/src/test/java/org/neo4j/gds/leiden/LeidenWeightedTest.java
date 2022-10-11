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

import org.junit.jupiter.api.RepeatedTest;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LeidenWeightedTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (nAlice:User)," +
        "  (nBridget:User)," +
        "  (nCharles:User)," +
        "  (nDoug:User)," +
        "  (nMark:User)," +
        "  (nMichael:User)," +

        "  (nAlice)-[:LINK {weight: 1}]->(nBridget)," +
        "  (nAlice)-[:LINK {weight: 1}]->(nCharles)," +
        "  (nCharles)-[:LINK {weight: 1}]->(nBridget)," +

        "  (nAlice)-[:LINK {weight: 5}]->(nDoug)," +

        "  (nMark)-[:LINK {weight: 1}]->(nDoug)," +
        "  (nMark)-[:LINK {weight: 1}]->(nMichael)," +
        "  (nMichael)-[:LINK {weight: 1}]->(nMark)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @RepeatedTest(10)
    void weightedLeiden() {
        var maxLevels = 10;
        Leiden leiden = new Leiden(
            graph,
            maxLevels,
            1.0,
            0.01,
            true,
            19L,
            null,
            4,
            ProgressTracker.NULL_TRACKER
        );

        var leidenResult = leiden.compute();

        assertThat(leidenResult.ranLevels()).isLessThanOrEqualTo(maxLevels);
        assertThat(leidenResult.didConverge()).isTrue();

        var communities = leidenResult.communities();

        var communitiesMap = Stream.of("nAlice", "nBridget", "nCharles", "nDoug", "nMark", "nMichael")
            .collect(Collectors.groupingBy(v -> communities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("nAlice", "nDoug"),
                community -> assertThat(community).containsExactlyInAnyOrder("nBridget", "nCharles"),
                community -> assertThat(community).containsExactlyInAnyOrder("nMark", "nMichael")
            );

    }
}
