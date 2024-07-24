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
package org.neo4j.gds.bridges;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class BridgesLargestTest {

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

    private Bridge bridge(String from, String to) {
        return new Bridge(graph.toOriginalNodeId(from), graph.toOriginalNodeId(to));
    }


    @Test
    void shouldFindAllBridges() {
        var bridges = new Bridges(graph, ProgressTracker.NULL_TRACKER);

        var result = bridges.compute().bridges().stream()
            .map(b -> new Bridge(
            graph.toOriginalNodeId(b.from()),
            graph.toOriginalNodeId(b.to())
        )).toList();


        assertThat(result)
            .isNotNull()
            .containsExactlyInAnyOrder(
                bridge("a1", "a2"),
                bridge("a3", "a4"),
                bridge("a3", "a7"),
                bridge("a7", "a8"),
                bridge("a10", "a11"),
                bridge("a14", "a13")
            );
    }
}
