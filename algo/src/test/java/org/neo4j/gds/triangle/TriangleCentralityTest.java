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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.GdlGraphStoreBuilder;
import org.neo4j.gds.GdlSupport;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.huge.NodeFilteredGraph;
import org.neo4j.gds.core.huge.UnionGraph;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.termination.TerminatedException;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class TriangleCentralityTest {

    /**
     * Single triangle: all vertices TC = 1.0
     * Triangle {a, b, c}: each vertex has 1 triangle, global = 1
     * For each vertex v: N△[v] = {v, other two}, N(v)\N△(v) = ∅
     * x = △(v) + △(u1) + △(u2) = 1 + 1 + 1 = 3
     * y = 0
     * TC(v) = (1/3 * 3) / 1 = 1.0
     */
    @Test
    void singleTriangle() {
        var graph = fromGdl(
            "CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(1);
        for (int i = 0; i < 3; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(1.0, within(1e-10));
        }
    }

    /**
     * Complete graph K5: all vertices TC = 1.0 (Observation 1)
     * There are C(5,3) = 10 triangles in total and C(4,2) = 6 per vertex.
     * All neighbors are triangle neighbors.
     */
    @Test
    void completeGraphK5() {
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b), " +
                "(a)-[:T]->(c), " +
                "(a)-[:T]->(d), " +
                "(a)-[:T]->(e), " +
                "(b)-[:T]->(c), " +
                "(b)-[:T]->(d), " +
                "(b)-[:T]->(e), " +
                "(c)-[:T]->(d), " +
                "(c)-[:T]->(e), " +
                "(d)-[:T]->(e)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(10);
        for (int i = 0; i < 5; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(1.0, within(1e-10));
        }
    }

    /**
     * Complete graph K4: all vertices TC = 1.0
     */
    @Test
    void completeGraphK4() {
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b), " +
                "(a)-[:T]->(c), " +
                "(a)-[:T]->(d), " +
                "(b)-[:T]->(c), " +
                "(b)-[:T]->(d), " +
                "(c)-[:T]->(d)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(4);
        for (int i = 0; i < 4; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(1.0, within(1e-10));
        }
    }

    /**
     * A windmill of three triangles sharing vertex v, TC(v) = 1.0.
     *
     * v is connected to a, b, c, d, e, f
     * Triangles: {v,a,b}, {v,c,d}, {v,e,f}
     * Global triangles = 3
     *
     * For v: △(v)=3, all neighbors are triangle neighbors
     *   x = △(v) + △(a) + △(b) + △(c) + △(d) + △(e) + △(f) = 3+1+1+1+1+1+1 = 9
     *   y = 0
     *   TC(v) = (1/3 * 9) / 3 = 1.0
     */
    @Test
    void windmillCenterVertex() {
        var testGraph = GdlSupport.fromGdl(
            "CREATE " +
                "(v)-[:T]->(a), " +
                "(v)-[:T]->(b), " +
                "(a)-[:T]->(b), " +
                "(v)-[:T]->(c), " +
                "(v)-[:T]->(d), " +
                "(c)-[:T]->(d), " +
                "(v)-[:T]->(e), " +
                "(v)-[:T]->(f), " +
                "(e)-[:T]->(f)",
            Orientation.UNDIRECTED
        );

        var result = compute(testGraph.graph());

        assertThat(result.globalTriangleCount()).isEqualTo(3);

        long vId = testGraph.toMappedNodeId("v");
        assertThat(result.centralities().get(vId)).isCloseTo(1.0, within(1e-10));

        // Leaf vertices (a, b, c, d, e, f) each have △=1
        // For a: N△[a] = {a, v, b}, N(a)\N△(a) = ∅
        //   x = △(a) + △(v) + △(b) = 1 + 3 + 1 = 5
        //   y = 0
        //   TC(a) = (1/3 * 5) / 3 = 5/9
        for (String name : new String[]{"a", "b", "c", "d", "e", "f"}) {
            long id = testGraph.toMappedNodeId(name);
            assertThat(result.centralities().get(id)).isCloseTo(5.0 / 9.0, within(1e-10));
        }
    }

    /**
     * Two disjoint triangles: Observation 3 — vertex in one of p=2 disjoint K3's → TC = 1/p = 0.5
     */
    @Test
    void twoDisjointTriangles() {
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
                "(d)-[:T]->(e)-[:T]->(f)-[:T]->(d)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(2);
        for (int i = 0; i < 6; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(0.5, within(1e-10));
        }
    }

    /**
     * Three disjoint triangles: TC = 1/3
     */
    @Test
    void threeDisjointTriangles() {
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
                "(d)-[:T]->(e)-[:T]->(f)-[:T]->(d), " +
                "(g)-[:T]->(h)-[:T]->(i)-[:T]->(g)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(3);
        for (int i = 0; i < 9; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(1.0 / 3.0, within(1e-10));
        }
    }

    /**
     * No triangles: all vertices TC = 0
     */
    @Test
    void noTriangles() {
        var graph = fromGdl(
            "CREATE (a)-[:T]->(b)-[:T]->(c)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(0);
        for (int i = 0; i < 3; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(0.0, within(1e-10));
        }
    }

    @Test
    void labelFilterMatchesTriangleCount() {
        var testGraph = GdlSupport.fromGdl(
            "CREATE " +
                "(a1:A)-[:T]->(a2:B), " +
                "(a2)-[:T]->(a3:C), " +
                "(a3)-[:T]->(a1), " +
                "(b1:D)-[:T]->(b2:E), " +
                "(b2)-[:T]->(b3:F), " +
                "(b3)-[:T]->(b1)",
            Orientation.UNDIRECTED
        );

        var graph = testGraph.graph();
        for (List<String> labelFilter : List.of(
            List.of("A", "B", "C"),
            List.of("B", "A", "C"),
            List.of("A"),
            List.of("A", "B")
        )) {
            var triangleCentrality = compute(graph, new Concurrency(4), Long.MAX_VALUE, labelFilter);
            var triangleCount = IntersectingTriangleCount.create(
                graph,
                new Concurrency(4),
                Long.MAX_VALUE,
                labelFilter,
                DefaultPool.INSTANCE,
                ProgressTracker.NULL_TRACKER,
                TerminationFlag.RUNNING_TRUE
            ).compute();

            assertThat(triangleCentrality.globalTriangleCount()).isEqualTo(triangleCount.globalTriangles());
            for (long node = 0; node < graph.nodeCount(); node++) {
                assertThat(triangleCentrality.triangleCounts().get(node))
                    .isEqualTo(triangleCount.localTriangles().get(node));
            }
        }
    }

    /**
     * Isolated vertex in a graph with triangles: TC = 0
     * Triangle {a,b,c} + isolated vertex d
     */
    @Test
    void isolatedVertex() {
        var graph = fromGdl(
            "CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a), (d)"
        );

        var result = compute(graph);

        assertThat(result.globalTriangleCount()).isEqualTo(1);
        // Vertices in the triangle: TC = 1.0
        assertThat(result.centralities().get(0)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(1)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(2)).isCloseTo(1.0, within(1e-10));
        // Isolated vertex: TC = 0
        assertThat(result.centralities().get(3)).isCloseTo(0.0, within(1e-10));
    }

    /**
     * Leaf d belongs to no triangle but receives the full weight of a's triangle count.
     * Its score is therefore 1, just like the triangle's vertices. Treating every neighbor
     * as a triangle neighbor would incorrectly divide d's contribution by three.
     */
    @Test
    void nonTriangleNeighborContribution() {
        var testGraph = GdlSupport.fromGdl(
            "CREATE " +
                "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
                "(a)-[:T]->(d)",
            Orientation.UNDIRECTED
        );

        var result = compute(testGraph.graph());

        assertThat(result.globalTriangleCount()).isEqualTo(1);

        long aId = testGraph.toMappedNodeId("a");
        long bId = testGraph.toMappedNodeId("b");
        long cId = testGraph.toMappedNodeId("c");
        long dId = testGraph.toMappedNodeId("d");

        assertThat(result.centralities().get(aId)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(bId)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(cId)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(dId)).isCloseTo(1.0, within(1e-10));
    }

    @Test
    void ignoresSelfLoops() {
        var testGraph = GdlSupport.fromGdl(
            "CREATE " +
                "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
                "(a)-[:T]->(a)",
            Orientation.UNDIRECTED
        );

        var result = compute(testGraph.graph());

        assertThat(result.globalTriangleCount()).isEqualTo(1);

        long aId = testGraph.toMappedNodeId("a");
        long bId = testGraph.toMappedNodeId("b");
        long cId = testGraph.toMappedNodeId("c");

        assertThat(result.allNeighbors().get(aId)).isEqualTo(2);
        assertThat(result.allNeighborsTriangleSums().get(aId)).isEqualTo(2);
        assertThat(result.centralities().get(aId)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(bId)).isCloseTo(1.0, within(1e-10));
        assertThat(result.centralities().get(cId)).isCloseTo(1.0, within(1e-10));
    }

    @Test
    void supportsUnionGraphsAndDeduplicatesRelationshipsAcrossTypes() {
        var testGraph = GdlSupport.fromGdl(
            "CREATE " +
                "(a)-[:T1]->(b)-[:T1]->(c)-[:T2]->(a), " +
                "(a)-[:T2]->(b)",
            Orientation.UNDIRECTED
        );

        var result = compute(testGraph.graph());

        assertThat(result.globalTriangleCount()).isEqualTo(1);
        for (int i = 0; i < 3; i++) {
            assertThat(result.triangleCounts().get(i)).isEqualTo(1);
            assertThat(result.triangleNeighbors().get(i)).isEqualTo(2);
            assertThat(result.allNeighbors().get(i)).isEqualTo(2);
            assertThat(result.centralities().get(i)).isCloseTo(1.0, within(1e-10));
        }
    }

    @Test
    void excludesEveryTriangleIncidentToANodeAboveMaxDegree() {
        // Excluding a and b destroys both triangles in their component; c and d remain included
        // with zero scores. The separate e-f-g triangle must still supply the global denominator.
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b), " +
                "(a)-[:T]->(c), " +
                "(a)-[:T]->(d), " +
                "(b)-[:T]->(c), " +
                "(b)-[:T]->(d), " +
                "(e)-[:T]->(f)-[:T]->(g)-[:T]->(e)"
        );

        var result = compute(graph, new Concurrency(4), 2);

        assertThat(result.globalTriangleCount()).isEqualTo(1);
        assertThat(result.triangleCounts().get(0)).isEqualTo(IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT);
        assertThat(result.triangleCounts().get(1)).isEqualTo(IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT);
        assertThat(result.centralities().get(0)).isNaN();
        assertThat(result.centralities().get(1)).isNaN();

        assertThat(result.triangleCounts().get(2)).isZero();
        assertThat(result.triangleCounts().get(3)).isZero();
        assertThat(result.centralities().get(2)).isZero();
        assertThat(result.centralities().get(3)).isZero();

        for (int node = 4; node < 7; node++) {
            assertThat(result.triangleCounts().get(node)).isEqualTo(1);
            assertThat(result.centralities().get(node)).isCloseTo(1.0, within(1e-10));
        }
    }

    /**
     * Without label filtering, TC values should be in [0, 1] (Proposition 1 from paper).
     */
    @Test
    void centralityValuesInUnitInterval() {
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
                "(c)-[:T]->(d)-[:T]->(e)-[:T]->(c), " +
                "(e)-[:T]->(f), " +
                "(g)"
        );

        var result = compute(graph);

        for (int i = 0; i < graph.nodeCount(); i++) {
            double tc = result.centralities().get(i);
            assertThat(tc).isBetween(0.0, 1.0);
        }
    }

    /**
     * K4's triangles share edges and vertices, allowing intersection workers to update the
     * same counters and edge marks. Worker count must not change the four-triangle total.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8})
    void parallelCorrectness(int threads) {
        var graph = fromGdl(
            "CREATE " +
                "(a)-[:T]->(b), " +
                "(a)-[:T]->(c), " +
                "(a)-[:T]->(d), " +
                "(b)-[:T]->(c), " +
                "(b)-[:T]->(d), " +
                "(c)-[:T]->(d)"
        );

        var result = compute(graph, new Concurrency(threads));

        assertThat(result.globalTriangleCount()).isEqualTo(4);
        for (int i = 0; i < 4; i++) {
            assertThat(result.centralities().get(i)).isCloseTo(1.0, within(1e-10));
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void labelFilterKeepsCentralityNormalized(boolean multipleRelationshipTypes, boolean nodeFiltered) {
        // Only A-B-C is counted, but D still shares triangle edges with all three counted nodes.
        // Losing those marks would give D a full-weight sum of 3 instead of its correct score of 1.
        // Exercise HugeGraph, UnionGraph, NodeFilteredGraph, and a node-filtered union.
        String secondType = multipleRelationshipTypes ? "U" : "T";
        var store = new GdlGraphStoreBuilder()
            .gdl("CREATE (a:A:Keep), (b:B:Keep), (c:C:Keep), (d:D:Keep), (outside:Drop), " +
                "(a)-[:T]->(b), (a)-[:" + secondType + "]->(c), (a)-[:T]->(d), " +
                "(b)-[:T]->(c), (b)-[:" + secondType + "]->(d), (c)-[:T]->(d), (a)-[:T]->(outside)")
            .orientation(Orientation.UNDIRECTED)
            .build();
        var graph = store.getGraph(
            nodeFiltered ? List.of(NodeLabel.of("Keep")) : store.nodeLabels(),
            store.relationshipTypes(),
            Optional.empty()
        );
        assertThat(graph).isInstanceOf(multipleRelationshipTypes ? UnionGraph.class
            : nodeFiltered ? NodeFilteredGraph.class : HugeGraph.class);

        var result = compute(graph, new Concurrency(4), Long.MAX_VALUE, List.of("A", "B", "C"));

        assertThat(result.globalTriangleCount()).isEqualTo(1);
        for (long node = 0; node < 4; node++) {
            assertThat(result.triangleCounts().get(node)).isEqualTo(node == 3 ? 0 : 1);
            assertThat(result.triangleNeighbors().get(node)).isEqualTo(3);
            assertThat(result.triangleNeighborsTriangleSums().get(node)).isEqualTo(node == 3 ? 3 : 2);
            assertThat(result.centralities().get(node)).isEqualTo(1.0);
        }
    }

    @Test
    void marksEdgesAcrossSourceBatchBoundary() {
        // Create center last so its adjacency list owns every spoke's marker. One triangle's
        // spokes fit in the local bitmap; the other's lie beyond it and must be marked directly.
        int leaves = TriangleCentrality.SOURCE_EDGE_BATCH_BITS + 2;
        var gdl = new StringBuilder("CREATE (n0)");
        for (int i = 1; i < leaves; i++) {
            gdl.append(", (n").append(i).append(")");
        }
        gdl.append(", (center)");
        for (int i = 0; i < leaves; i++) {
            gdl.append(", (center)-[:T]->(n").append(i).append(")");
        }
        gdl.append(", (n0)-[:T]->(n1), (n").append(leaves - 2).append(")-[:T]->(n")
            .append(leaves - 1).append(")");
        var graph = GdlSupport.fromGdl(gdl.toString(), Orientation.UNDIRECTED);

        var result = compute(graph.graph());

        long center = graph.toMappedNodeId("center");
        assertThat(result.globalTriangleCount()).isEqualTo(2);
        assertThat(result.triangleNeighbors().get(center)).isEqualTo(4);
        assertThat(result.allNeighbors().get(center)).isEqualTo(leaves);
        assertThat(result.centralities().get(center)).isEqualTo(1.0);
        for (int i : new int[]{0, 1, leaves - 2, leaves - 1}) {
            assertThat(result.centralities().get(graph.toMappedNodeId("n" + i)))
                .isCloseTo(2.0 / 3.0, within(1e-10));
        }
        assertThat(result.centralities().get(graph.toMappedNodeId("n2"))).isEqualTo(1.0);
    }

    @Test
    void shouldReportProgressForExcludedNodes() {
        // Exclude every node to exercise the skip paths. The intermediate 33% message ensures
        // progress is reported during traversal, rather than only when endSubTask forces 100%.
        var graph = fromGdl("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)");
        var concurrency = new Concurrency(1);
        var tracker = TestProgressTrackerHelper.create(
            TriangleCentralityProgressTrackerCreator.progressTask(concurrency, graph.nodeCount()),
            concurrency
        );

        TriangleCentrality.create(
            graph, concurrency, 1, DefaultPool.INSTANCE, tracker.progressTracker(), TerminationFlag.RUNNING_TRUE
        ).compute();

        for (String phase : List.of(
            "Index adjacency offsets", "Count triangles", "Aggregate triangle neighbors", "Compute centrality"
        )) {
            tracker.log().assertContainsMessage(TestLog.INFO, "TriangleCentrality :: " + phase + " 33%");
            tracker.log().assertContainsMessage(TestLog.INFO, "TriangleCentrality :: " + phase + " 100%");
        }
    }

    @Test
    void shouldRespectTerminationDuringIndexing() {
        // Cancel after the first prefix-sum entry so termination must be observed by indexing
        // itself, before the intersection workers have a chance to check the flag.
        var graph = fromGdl("CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)");
        var running = new AtomicBoolean(true);
        var tracker = mock(ProgressTracker.class);
        doAnswer(invocation -> {
            running.set(false);
            return null;
        }).when(tracker).onProgress();

        assertThatExceptionOfType(TerminatedException.class).isThrownBy(() -> TriangleCentrality.create(
            graph, new Concurrency(1), Long.MAX_VALUE, DefaultPool.INSTANCE, tracker, running::get
        ).compute());
    }

    private TriangleCentralityResult compute(Graph graph) {
        return compute(graph, new Concurrency(4));
    }

    private TriangleCentralityResult compute(Graph graph, Concurrency concurrency) {
        return compute(graph, concurrency, Long.MAX_VALUE);
    }

    private TriangleCentralityResult compute(Graph graph, Concurrency concurrency, long maxDegree) {
        return TriangleCentrality.create(
            graph,
            concurrency,
            maxDegree,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();
    }

    private TriangleCentralityResult compute(
        Graph graph,
        Concurrency concurrency,
        long maxDegree,
        List<String> labelFilter
    ) {
        return TriangleCentrality.create(
            graph,
            concurrency,
            maxDegree,
            labelFilter,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).compute();
    }

    private static Graph fromGdl(String gdl) {
        return GdlSupport.fromGdl(gdl, Orientation.UNDIRECTED).graph();
    }
}
