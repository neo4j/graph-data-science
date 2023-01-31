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
import org.neo4j.gds.api.schema.Direction;
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
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@GdlExtension
class GraphWithSelfLoopTest {
    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node)," +
        "  (a1:Node)," +
        "  (a2:Node)," +
        "  (a0)-[:R]->(a0)," +
        "  (a0)-[:R]->(a1)," +
        "  (a1)-[:R]->(a2)";

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldCalculateModularityCorrectly() {
        var localCommunities = HugeLongArray.of(0, 0, 2);
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
        assertThat(modularity).isCloseTo(-0.0555555, Offset.offset(1e-4));
    }
    @Test
    void shouldAggregateCorrectly(){
        var communities = HugeLongArray.of(0, 0, 2);

        var aggregationPhase = new GraphAggregationPhase(
            graph,
            Direction.UNDIRECTED,
            communities,
            2L,
            Pools.DEFAULT_SINGLE_THREAD_POOL,
            4,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        var aggregatedGraph = aggregationPhase.run();

        assertThat(aggregatedGraph.nodeCount()).isEqualTo(2);

        aggregatedGraph.forEachNode(nodeId -> {
            assertThat(aggregatedGraph.toOriginalNodeId(nodeId)).isIn(0L, 2L);
            return true;
        });

        assertGraphEquals(
            fromGdl(
                "(c1), (c2), " +
                "(c1)-[:REL {w: 1.0}]->(c2), " +
                "(c2)-[:REL {w: 1.0}]->(c1)"
            ),
            aggregatedGraph
        );
    }
    @Test
    void shouldMaintainPartition() {
        var localCommunities = HugeLongArray.of(0, 0, 2);
        var refinedCommunities = HugeLongArray.of(0, 0, 2);
        var aggregationPhase = new GraphAggregationPhase(
            graph,
            Direction.UNDIRECTED,
            refinedCommunities,
            2,
            Pools.DEFAULT_SINGLE_THREAD_POOL,
            1,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );
        var nodeCount = graph.nodeCount();
        HugeDoubleArray refinedVolumes = HugeDoubleArray.of(5, 0, 1);
        var workingGraph = aggregationPhase.run();
        var maintainResult = Leiden.maintainPartition(
            workingGraph,
            localCommunities,
            refinedVolumes,
            nodeCount
        );

        assertThat(maintainResult.seededCommunitiesForNextIteration.toArray()).containsExactly(0, 1);
        assertThat(maintainResult.aggregatedNodeSeedVolume.toArray()).containsExactly(5, 1);
        assertThat(maintainResult.communityVolumes.toArray()).containsExactly(5, 1);

    }



    @Test
    void shouldCalculateModularityInSummaryGraph() {
        var localCommunities = HugeLongArray.of(0, 0, 2);
        var communityVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        graph.forEachNode(v -> {
            communityVolumes.addTo(localCommunities.get(v), graph.degree(v));
            return true;
        });
        var aggregateGraph = new GraphAggregationPhase(
            graph,
            Direction.UNDIRECTED,
            localCommunities,
            2,
            Pools.DEFAULT,
            1,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        ).run();
        var localCommunitiesCondensed = HugeLongArray.of(0, 1);
        var communityVolumes2 = HugeDoubleArray.of(communityVolumes.get(0), communityVolumes.get(2));
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
        assertThat(modularity).isCloseTo(-0.0555555, Offset.offset(1e-4));
    }
}
