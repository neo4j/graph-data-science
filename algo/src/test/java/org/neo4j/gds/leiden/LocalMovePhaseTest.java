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
class LocalMovePhaseTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a3:Node)," +
        "  (a4:Node)," +
        "  (a5:Node)," +
        "  (a6:Node)," +
        "  (a7:Node)," +
        "  (a0)-[:R {weight: 3.0}]->(a1)," +
        "  (a0)-[:R {weight: 1.5}]->(a2)," +
        "  (a0)-[:R {weight: 1.5}]->(a3)," +
        "  (a0)-[:R {weight: 1.5}]->(a4)," +
        "  (a2)-[:R {weight: 3.0}]->(a3)," +
        "  (a2)-[:R {weight: 3.0}]->(a4)," +
        "  (a3)-[:R {weight: 3.0}]->(a4)," +
        "  (a1)-[:R {weight: 1.5}]->(a5)," +
        "  (a1)-[:R {weight: 1.5}]->(a6)," +
        "  (a1)-[:R {weight: 1.5}]->(a7)," +
        "  (a5)-[:R {weight: 3.0}]->(a6)," +
        "  (a5)-[:R {weight: 3.0}]->(a7)," +
        "  (a6)-[:R {weight: 3.0}]->(a7)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testLocalMovePhase() {

        var nodeVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        nodeVolumes.setAll(graph::degree);

        var communityVolumes = nodeVolumes.copyOf(graph.nodeCount());

        double gamma = 1.0 / graph.relationshipCount();

        HugeLongArray communities = LeidenUtils.createSingleNodeCommunities(graph.nodeCount());
        LocalMovePhase.create(
            graph,
            communities,
            nodeVolumes,
            communityVolumes,
            gamma,
            graph.nodeCount(),
            1
        ).run();


        var communitiesMap = LongStream
            .range(0, 8)
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> communities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .hasSize(2)
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("a0", "a2", "a3", "a4"),
                community -> assertThat(community).containsExactlyInAnyOrder("a1", "a5", "a6", "a7")
            );
    }

}
