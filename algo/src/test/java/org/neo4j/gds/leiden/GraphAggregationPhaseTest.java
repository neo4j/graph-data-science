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
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;

@GdlExtension
class GraphAggregationPhaseTest {


    @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 0)
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
        "  (a2)-[:R {weight: 1.0}]->(a7)," +
        "  (a0)-[:R {weight: 1.5}]->(a2)," +
        "  (a0)-[:R {weight: 1.5}]->(a3)," +
        "  (a0)-[:R {weight: 1}]->(a4)," +
        "  (a2)-[:R {weight: 3.0}]->(a3)," +
        "  (a2)-[:R {weight: 3.0}]->(a4)," +
        "  (a3)-[:R {weight: 3.0}]->(a4)," +
        "  (a1)-[:R {weight: 1.5}]->(a5)," +
        "  (a1)-[:R {weight: 1.5}]->(a6)," +
        "  (a1)-[:R {weight: 1}]->(a7)," +
        "  (a5)-[:R {weight: 3.0}]->(a6)," +
        "  (a5)-[:R {weight: 3.0}]->(a7)," +
        "  (a6)-[:R {weight: 3.0}]->(a7)";

    @Inject
    private TestGraph graph;

    @Test
    void testGraphAggregation() {
        var communities = HugeLongArray.of(0, 1, 0, 0, 0, 1, 1, 1);

        var aggregationPhase = new GraphAggregationPhase(
            graph,
            Direction.UNDIRECTED,
            communities,
            1L,
            Pools.DEFAULT_SINGLE_THREAD_POOL,
            4,
            TerminationFlag.RUNNING_TRUE,
            ProgressTracker.NULL_TRACKER
        );

        var aggregatedGraph = aggregationPhase.run();

        assertThat(aggregatedGraph.nodeCount()).isEqualTo(2);

        aggregatedGraph.forEachNode(nodeId -> {
            assertThat(aggregatedGraph.toOriginalNodeId(nodeId)).isIn(0L, 1L);
            return true;
        });

        // FIXME why do the weights differ now?
        assertGraphEquals(
            fromGdl(
                "(c1), (c2), " +
                "(c1)-[:_IGNORED_ {w: 4.0}]->(c2), " +
                "(c2)-[:_IGNORED_ {w: 4.0}]->(c1)"
            ),
            aggregatedGraph
        );
    }

    @Test
    void testNodesSortedByCommunity() {
        //concurrency 1 examines  nodes with ordering  nodes 4,3,1,0,2,6,5,8,7
        HugeLongArray communities = HugeLongArray.of(0, 1, 3, 3, 3, 1, 0, 0, 0);
        var sortedNodeByCommunity = GraphAggregationPhase.getNodesSortedByCommunity(communities, 1);
        var expected = new long[]{2, 3, 4, 7, 8, 6, 0, 5, 1};
        assertThat(sortedNodeByCommunity.toArray()).isEqualTo(expected);

    }

    @Test
    void testNodesSortedByCommunityWithConcurrency() {

        HugeLongArray communities = HugeLongArray.of(0, 1, 3, 3, 3, 1, 0, 0, 0);
        var sortedNodeByCommunity = GraphAggregationPhase.getNodesSortedByCommunity(communities, 4);
        long[] actual = sortedNodeByCommunity.toArray();
        assertThat(actual).containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8);
        HashSet<Long> forbidden = new HashSet<>();
        long current = -1;
        for (int i = 0; i < 9; ++i) {
            long nodeId = actual[i];
            long community = communities.get(nodeId);

            assertThat(community).isNotIn(forbidden); //true only if all nodes of community are consecutive
            if (current != community) {
                forbidden.add(current);
                current = community;
            }
        }

    }

}
