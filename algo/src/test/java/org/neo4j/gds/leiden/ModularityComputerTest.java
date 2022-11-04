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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ModularityComputerTest {
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
        "  (a0)-[:R {weight: 1.0}]->(a1)," +
        "  (a0)-[:R {weight: 1.0}]->(a2)," +
        "  (a0)-[:R {weight: 1.0}]->(a3)," +
        "  (a0)-[:R {weight: 1.0}]->(a4)," +
        "  (a2)-[:R {weight: 1.0}]->(a3)," +
        "  (a2)-[:R {weight: 1.0}]->(a4)," +
        "  (a3)-[:R {weight: 1.0}]->(a4)," +
        "  (a1)-[:R {weight: 1.0}]->(a5)," +
        "  (a1)-[:R {weight: 1.0}]->(a6)," +
        "  (a1)-[:R {weight: 1.0}]->(a7)," +
        "  (a5)-[:R {weight: 1.0}]->(a6)," +
        "  (a5)-[:R {weight: 1.0}]->(a7)," +
        "  (a6)-[:R {weight: 1.0}]->(a7)";

    //HugeLongArray.of(1, 1, 1, 3, 3, 3, 1, 3);
    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;


    @Test
    void shouldCalculateModularityCorrectly() {
        var localCommunities = HugeLongArray.of(0, 1, 0, 0, 0, 1, 1, 1);
        var communityVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        graph.forEachNode(v -> {
            communityVolumes.addTo(localCommunities.get(v), graph.degree(v));
            return true;
        });
        double modularity = ModularityComputer.compute(
            graph,
            localCommunities,
            communityVolumes,
            1.0 / graph.relationshipCount(),
            1.0 / graph.relationshipCount(),
            4,
            Pools.DEFAULT,
            ProgressTracker.EmptyProgressTracker.NULL_TRACKER
        );
        assertThat(modularity).isCloseTo(0.4230, Offset.offset(1e-3));
    }

    @Test
    void shouldCalculateModularityInSummaryGraph() {
        var localCommunities = HugeLongArray.of(0, 1, 0, 0, 0, 1, 1, 1);
        var communityVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        graph.forEachNode(v -> {
            communityVolumes.addTo(localCommunities.get(v), graph.degree(v));
            return true;
        });
        var aggregateGraph = new GraphAggregationPhase(
            graph,
            Orientation.UNDIRECTED,
            localCommunities,
            1,
            Pools.DEFAULT,
            1,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        ).run();
        var localCommunitiesCondensed = HugeLongArray.of(0, 1);
        var communityVolumes2 = HugeDoubleArray.of(communityVolumes.get(0), communityVolumes.get(1));
        double modularity = ModularityComputer.compute(
            aggregateGraph,
            localCommunitiesCondensed,
            communityVolumes2,
            1.0 / graph.relationshipCount(),
            1.0 / graph.relationshipCount(),
            4,
            Pools.DEFAULT,
            ProgressTracker.EmptyProgressTracker.NULL_TRACKER
        );
        assertThat(modularity).isCloseTo(0.4230, Offset.offset(1e-3));
    }
}
