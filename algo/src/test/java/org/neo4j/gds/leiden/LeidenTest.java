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
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class LeidenTest {


    @GdlGraph(orientation = Orientation.UNDIRECTED)
    private static final String DB_CYPHER =
        "CREATE " +
        "  (a0:Node {optimal: 5000, seed: 1})," +
        "  (a1:Node {optimal: 4000,seed: 2})," +
        "  (a2:Node {optimal: 5000,seed: 3})," +
        "  (a3:Node {optimal: 5000})," +
        "  (a4:Node {optimal: 5000,seed: 5})," +
        "  (a5:Node {optimal: 4000,seed: 6})," +
        "  (a6:Node {optimal: 4000,seed: 7})," +
        "  (a7:Node {optimal: 4000,seed: 8})," +
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

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void leiden() {
        int maxLevels = 3;
        Leiden leiden = new Leiden(
            graph,
            maxLevels,
            1.0,
            0.01,
            false,
            19L,
            null,
            1,
            ProgressTracker.NULL_TRACKER
        );

        var leidenResult = leiden.compute();

        assertThat(leidenResult.ranLevels()).isLessThanOrEqualTo(maxLevels);
        assertThat(leidenResult.didConverge()).isTrue();

        var communities = leidenResult.communities();
        var communitiesMap = LongStream
            .range(0, graph.nodeCount())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> communities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .hasSize(2)
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("a0", "a2", "a3", "a4"),
                community -> assertThat(community).containsExactlyInAnyOrder("a1", "a5", "a6", "a7")
            );
    }

    @Test
    void shouldWorkWithBestSeed() {
        int maxLevels = 3;
        Leiden leiden = new Leiden(
            graph,
            maxLevels,
            1.0,
            0.01,
            false,
            19L,
            graph.nodeProperties("optimal"),
            1,
            ProgressTracker.NULL_TRACKER
        );

        var leidenResult = leiden.compute();

        assertThat(leidenResult.ranLevels()).isEqualTo(1);
        assertThat(leidenResult.didConverge()).isTrue();

        var communities = leidenResult.communities();
        var communitiesMap = LongStream
            .range(0, graph.nodeCount())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> communities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .hasSize(2)
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("a0", "a2", "a3", "a4"),
                community -> assertThat(community).containsExactlyInAnyOrder("a1", "a5", "a6", "a7")
            );
        assertThat(communitiesMap.keySet()).containsExactly(4000L, 5000L);
    }

    @Test
    void shouldWorkWithMissingSeed() {
        int maxLevels = 3;
        Leiden leiden = new Leiden(
            graph,
            maxLevels,
            1.0,
            0.01,
            false,
            19L,
            graph.nodeProperties("seed"),
            1,
            ProgressTracker.NULL_TRACKER
        );

        var leidenResult = leiden.compute();

        assertThat(leidenResult.ranLevels()).isEqualTo(1);
        assertThat(leidenResult.didConverge()).isTrue();

        var communities = leidenResult.communities();
        var communitiesMap = LongStream
            .range(0, graph.nodeCount())
            .mapToObj(v -> "a" + v)
            .collect(Collectors.groupingBy(v -> communities.get(idFunction.of(v))));

        assertThat(communitiesMap.values())
            .hasSize(2)
            .satisfiesExactlyInAnyOrder(
                community -> assertThat(community).containsExactlyInAnyOrder("a0", "a2", "a3", "a4"),
                community -> assertThat(community).containsExactlyInAnyOrder("a1", "a5", "a6", "a7")
            );
    }

    @Test
    void shouldMaintainPartition() {
        var localCommunities = HugeLongArray.of(1, 1, 1, 3, 3, 3, 1, 3);
        var refinedCommunities = HugeLongArray.of(1, 1, 2, 3, 4, 3, 1, 3);
        var aggregationPhase = new GraphAggregationPhase(
            graph,
            Orientation.UNDIRECTED,
            refinedCommunities,
            4,
            Pools.DEFAULT_SINGLE_THREAD_POOL,
            1,
            TerminationFlag.RUNNING_TRUE
        );
        HugeDoubleArray refinedVolumes = HugeDoubleArray.newArray(graph.nodeCount());
        var workingGraph = aggregationPhase.run();
        var nextCommunities = Leiden.maintainPartition(
            workingGraph,
            localCommunities, refinedVolumes

        ).seededCommunitiesForNextIteration;
        assertThat(nextCommunities.toArray()).containsExactly(0, 0, 2, 2);

    }
}
