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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.concurrent.atomic.DoubleAdder;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class RelationshipCountCollectorTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static final String GRAPH =
        "CREATE " +
        " (a1: Node { communityId: 0 })," +
        " (a2: Node { communityId: 0 })," +
        " (a3: Node { communityId: 5 })," +
        " (a4: Node { communityId: 0 })," +
        " (a5: Node { communityId: 5 })," +
        " (a6: Node { communityId: 5 })," +

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

    @Test
    void collect() {
        var insideRelationships = HugeAtomicDoubleArray.newArray(graph.nodeCount());
        var totalCommunityRelationships = HugeAtomicDoubleArray.newArray(graph.nodeCount());
        var communityTracker = HugeAtomicBitSet.create(graph.nodeCount());
        var communities = HugeLongArray.of(0, 0, 5, 0, 5, 5);
        var totalRelationshipWeight = new DoubleAdder();

        new RelationshipCountCollector(
            new Partition(0L, graph.nodeCount()),
            graph,
            insideRelationships,
            totalCommunityRelationships,
            communityTracker,
            communities::get,
            totalRelationshipWeight
        ).run();

        assertThat(totalRelationshipWeight.doubleValue()).isEqualTo(16);
        assertThat(insideRelationships.get(0)).isEqualTo(6);
        assertThat(totalCommunityRelationships.get(0)).isEqualTo(9);
        assertThat(insideRelationships.get(5)).isEqualTo(4);
        assertThat(totalCommunityRelationships.get(5)).isEqualTo(7);
    }
}
