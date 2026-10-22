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
package org.neo4j.gds.triangle;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class TriangleCentralityFilteredGraphTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static final String DB_CYPHER =
        "CREATE " +
            "(a1:A), (a2:A), (a3:A), " +
            "(b1:B), (b2:B), (b3:B), " +
            "(a1)-[:R1]->(a2), (a2)-[:R2]->(a3), (a3)-[:R1]->(a1), " +
            "(b1)-[:R1]->(b2), (b2)-[:R2]->(b3), (b3)-[:R1]->(b1)";

    @GdlGraph(graphNamePrefix = "maxDegree", orientation = Orientation.UNDIRECTED)
    static final String MAX_DEGREE_DB_CYPHER =
        "CREATE " +
            "(b1:B), (b2:B), (b3:B), (outside:A), " +
            "(b1)-[:R]->(b2), (b2)-[:R]->(b3), (b3)-[:R]->(b1), " +
            "(b1)-[:R]->(outside)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private GraphStore maxDegreeGraphStore;

    @Test
    void supportsNodeFilteredUnionGraphs() {
        // B's nodes have different root and filtered IDs, and their triangle spans R1 and R2.
        // Opening an adjacency list with a filtered ID as a root ID can select A's component instead.
        var graph = graphStore.getGraph(
            List.of(NodeLabel.of("B")),
            List.of(RelationshipType.of("R1"), RelationshipType.of("R2")),
            Optional.empty()
        );

        var result = TriangleCentrality.create(
            graph,
            new Concurrency(4),
            Long.MAX_VALUE,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();

        var triangleCountResult = IntersectingTriangleCount.create(
            graph,
            new Concurrency(4),
            Long.MAX_VALUE,
            List.of(),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(result.globalTriangleCount()).isEqualTo(triangleCountResult.globalTriangles());
        for (long node = 0; node < graph.nodeCount(); node++) {
            assertThat(result.centralities().get(node)).isEqualTo(1.0);
            assertThat(result.triangleCounts().get(node)).isEqualTo(triangleCountResult.localTriangles().get(node));
            assertThat(result.triangleNeighbors().get(node)).isEqualTo(2);
            assertThat(result.allNeighbors().get(node)).isEqualTo(2);
            assertThat(result.triangleNeighborsTriangleSums().get(node)).isEqualTo(2);
            assertThat(result.allNeighborsTriangleSums().get(node)).isEqualTo(2);
        }
    }

    @Test
    void respectsUnderlyingMaxDegreeForNodeFilteredGraph() {
        // b1 has degree 3 before filtering and degree 2 afterward. Triangle Count still excludes
        // it from intersections, while all selected nodes retain count 0 rather than the -1 sentinel.
        var graph = maxDegreeGraphStore.getGraph(
            List.of(NodeLabel.of("B")),
            List.of(RelationshipType.of("R")),
            Optional.empty()
        );
        assertThat(graph).isInstanceOf(NodeFilteredGraph.class);

        var triangleCountResult = IntersectingTriangleCount.create(
            graph,
            new Concurrency(4),
            2,
            List.of(),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();
        var triangleCentralityResult = TriangleCentrality.create(
            graph,
            new Concurrency(4),
            2,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(triangleCountResult.globalTriangles()).isZero();
        assertThat(triangleCentralityResult.globalTriangleCount()).isEqualTo(triangleCountResult.globalTriangles());
        for (long node = 0; node < graph.nodeCount(); node++) {
            assertThat(triangleCountResult.localTriangles().get(node)).isZero();
            assertThat(triangleCentralityResult.triangleCounts().get(node))
                .isEqualTo(triangleCountResult.localTriangles().get(node));
            assertThat(triangleCentralityResult.triangleNeighbors().get(node)).isZero();
            assertThat(triangleCentralityResult.centralities().get(node)).isZero();
        }
    }
}
