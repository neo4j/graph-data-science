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
package org.neo4j.gds.articulationpoints;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.articulationPoints.ArticulationPointsParameters;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class ArticulationPointsLargerGraphTest {

    // https://upload.wikimedia.org/wikipedia/commons/d/df/Graph_cut_edges.svg
    @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 1)
    private static final String GRAPH =

        """
            CREATE
               (a1:Node),
               (a2:Node),
               (a3:Node),
               (a4:Node),
               (a5:Node),
               (a6:Node),
               (a7:Node),
               (a8:Node),
               (a9:Node),
               (a10:Node),
               (a11:Node),
               (a12:Node),
               (a13:Node),
               (a14:Node),
               (a15:Node),
               (a16:Node),
               (a1)-[:R]->(a2),
               (a3)-[:R]->(a4),
               (a3)-[:R]->(a7),
               (a7)-[:R]->(a8),
               (a5)-[:R]->(a9),
               (a5)-[:R]->(a10),
               (a9)-[:R]->(a10),
               (a9)-[:R]->(a14),
               (a10)-[:R]->(a11),
               (a11)-[:R]->(a12),
               (a10)-[:R]->(a14),
               (a11)-[:R]->(a15),
               (a12)-[:R]->(a16),
               (a13)-[:R]->(a14),
               (a15)-[:R]->(a16)
            """;

    @Inject
    private TestGraph graph;

    @Test
    void articulationPoints() {
        var articulationPoints =  ArticulationPoints.create(
            graph,
            new ArticulationPointsParameters(null, true),
            ProgressTracker.NULL_TRACKER
        );
        var result = articulationPoints.compute();

        var bitset= result.articulationPoints();
        var tracker = result.subtreeTracker().orElseThrow();

        assertThat(bitset.cardinality())
            .isEqualTo(5L);

        var a3=graph.toMappedNodeId("a3");
        var a7=graph.toMappedNodeId("a7");
        var a10=graph.toMappedNodeId("a10");
        var a11=graph.toMappedNodeId("a11");
        var a14=graph.toMappedNodeId("a14");

        assertThat(bitset.get(a3)).isTrue();
        assertThat(bitset.get(a7)).isTrue();
        assertThat(bitset.get(a10)).isTrue();
        assertThat(bitset.get(a11)).isTrue();
        assertThat(bitset.get(a14)).isTrue();

        assertThat(List.of(
                 tracker.minComponentSize(a3),
                 tracker.maxComponentSize(a3),
                 tracker.remainingComponents(a3))).
                 containsExactly(1L, 2L, 2L);

        assertThat(List.of(
            tracker.minComponentSize(a7),
            tracker.maxComponentSize(a7),
            tracker.remainingComponents(a7))).
            containsExactly(1L, 2L, 2L);

        assertThat(List.of(
            tracker.minComponentSize(a10),
            tracker.maxComponentSize(a10),
            tracker.remainingComponents(a10))).
            containsExactly(4L, 4L, 2L);

        assertThat(List.of(
            tracker.minComponentSize(a11),
            tracker.maxComponentSize(a11),
            tracker.remainingComponents(a11))).
            containsExactly(3L, 5L, 2L);

        assertThat(List.of(
            tracker.minComponentSize(a14),
            tracker.maxComponentSize(a14),
            tracker.remainingComponents(a14))).
            containsExactly(1L, 7L, 2L);
    }
}
