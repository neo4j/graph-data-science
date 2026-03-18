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
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.GdlSupport;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.triangle.IntersectingTriangleCount.EXCLUDED_NODE_TRIANGLE_COUNT;

class TriangleCentralityTriangleCountParityTest {

    private static final String LABEL_FILTER_GDL = "CREATE " +
        "(a1:A)-[:T]->(a2:B), (a2)-[:T]->(a3:C), (a3)-[:T]->(a1), " +
        "(a1)-[:T]->(a4:D), (a4)-[:T]->(a5:E), (a5)-[:T]->(a1)";
    private static final String SHARED_EDGE_GDL = "CREATE " +
        "(a)-[:T]->(b), (a)-[:T]->(c), (b)-[:T]->(c), " +
        "(a)-[:T]->(d), (b)-[:T]->(d), (a)-[:T]->(e)";

    // These graph shapes and configurations come from IntersectingTriangleCountTest.
    private static Stream<Scenario> triangleCountScenarios() {
        var scenarios = new ArrayList<Scenario>();
        scenarios.add(graph("line", "CREATE ()-[:T]->()-[:T]->()"));
        scenarios.add(graph("no relationships", "CREATE (), (), ()"));
        scenarios.add(graph("one relationship", "CREATE ()-[:T]->(), ()"));
        scenarios.add(graph("back and forth", "CREATE (a1)-[:T]->()-[:T]->(a1), ()"));

        for (int triangleCount : List.of(1, 2, 4, 8, 100)) {
            var gdl = new StringBuilder("CREATE ");
            for (int i = 0; i < triangleCount; i++) {
                gdl.append("(a").append(i).append(")-[:T]->()-[:T]->()-[:T]->(a").append(i).append(") ");
            }
            scenarios.add(graph("independent triangles " + triangleCount, gdl.toString()));
        }

        scenarios.add(graph("clique 5", "CREATE " +
            "(a1)-[:T]->(a2), (a1)-[:T]->(a3), (a1)-[:T]->(a4), (a1)-[:T]->(a5), " +
            "(a2)-[:T]->(a3), (a2)-[:T]->(a4), (a2)-[:T]->(a5), " +
            "(a3)-[:T]->(a4), (a3)-[:T]->(a5), (a4)-[:T]->(a5)"));

        scenarios.add(graph("label order ABC", LABEL_FILTER_GDL, List.of("A", "B", "C")));
        scenarios.add(graph("label order ACB", LABEL_FILTER_GDL, List.of("A", "C", "B")));
        scenarios.add(graph("label order BAC", LABEL_FILTER_GDL, List.of("B", "A", "C")));
        scenarios.add(graph("label order BCA", LABEL_FILTER_GDL, List.of("B", "C", "A")));
        scenarios.add(graph("label order CAB", LABEL_FILTER_GDL, List.of("C", "A", "B")));
        scenarios.add(graph("label order CBA", LABEL_FILTER_GDL, List.of("C", "B", "A")));

        var duplicateLabels = "CREATE " +
            "(a1:A)-[:T]->(a2:A), (a2)-[:T]->(a3:C), (a3)-[:T]->(a1), " +
            "(a1)-[:T]->(a4:D), (a4)-[:T]->(a5:E), (a5)-[:T]->(a1)";
        scenarios.add(graph("duplicate labels AAC", duplicateLabels, List.of("A", "A", "C")));
        scenarios.add(graph("duplicate labels ACA", duplicateLabels, List.of("A", "C", "A")));
        scenarios.add(graph("duplicate labels AAA", "CREATE " +
            "(a1:A)-[:T]->(a2:A), (a2)-[:T]->(a3:A), (a3)-[:T]->(a1), " +
            "(a1)-[:T]->(a4:D), (a4)-[:T]->(a5:E), (a5)-[:T]->(a1)", List.of("A", "A", "A")));
        scenarios.add(graph("multilabel node", "CREATE " +
            "(a1:A:B)-[:T]->(a2:A), (a2)-[:T]->(a3:C), (a3)-[:T]->(a1)", List.of("A", "B", "C")));

        var fewerLabels = "CREATE " +
            "(a1:A)-[:T]->(a2:A), (a2)-[:T]->(a3:B), (a3)-[:T]->(a1)";
        scenarios.add(graph("one label", fewerLabels, List.of("A")));
        scenarios.add(graph("two same labels", fewerLabels, List.of("A", "A")));
        scenarios.add(graph("two different labels", fewerLabels, List.of("A", "B")));

        scenarios.add(graph("clique 5 union", "CREATE " +
            "(a1)-[:T1]->(a2), (a1)-[:T1]->(a3), (a1)-[:T2]->(a4), (a1)-[:T3]->(a5), " +
            "(a2)-[:T4]->(a3), (a2)-[:T2]->(a4), (a2)-[:T2]->(a5), " +
            "(a3)-[:T3]->(a4), (a3)-[:T1]->(a5), (a4)-[:T4]->(a5)"));
        scenarios.add(graph("two triangles sharing a node", "CREATE " +
            "(a)-[:T]->()-[:T]->()-[:T]->(a), (a)-[:T]->()-[:T]->()-[:T]->(a)"));
        scenarios.add(graph("two triangles joined by a line", "CREATE " +
            "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
            "(q)-[:T]->(r)-[:T]->(t)-[:T]->(q), (a)-[:T]->(q)"));
        scenarios.add(graph("only self loops", "CREATE (a)-[:T]->(a)-[:T]->(a)-[:T]->(a)"));
        scenarios.add(graph("triangle with self loop", "CREATE (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)-[:T]->(a)"));
        scenarios.add(graph("parallel relationship", "CREATE " +
            "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), (a)-[:T]->(b)"));
        scenarios.add(graph("parallel triangles", "CREATE " +
            "(a)-[:T]->(b)-[:T]->(c)-[:T]->(a), (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)"));

        var cursorPrefix = "CREATE " +
            "(n0)-[:REL]->(n1), (n1)-[:REL]->(n2), (n0)-[:REL]->(n3)";
        scenarios.add(graph("triangle after first path", cursorPrefix + ", (n1)-[:REL]->(n3)"));
        scenarios.add(graph("triangle after another neighbor", cursorPrefix +
            ", (n0)-[:REL]->(n4), (n1)-[:REL]->(n3)"));
        scenarios.add(graph("triangle after most neighbors", cursorPrefix +
            ", (n0)-[:REL]->(n4), (n0)-[:REL]->(n5), (n1)-[:REL]->(n3)"));
        scenarios.add(graph("triangle at end of relationship chain", cursorPrefix +
            ", (n0)-[:REL]->(n4), (n1)-[:REL]->(n4)"));
        scenarios.add(graph("first member has more neighbors", cursorPrefix +
            ", (n0)-[:REL]->(n4), (n0)-[:REL]->(n5), (n1)-[:REL]->(n4), (n1)-[:REL]->(n6)"));

        scenarios.add(graph("first excluded intersection node", "CREATE " +
            "(n0)-[:REL]->(n1), (n1)-[:REL]->(n2), (n2)-[:REL]->(n3), " +
            "(n2)-[:REL]->(n4), (n2)-[:REL]->(n5), (n3)-[:REL]->(n4), " +
            "(n1)-[:REL]->(n6), (n0)-[:REL]->(n2), (n0)-[:REL]->(n6)", 3));
        scenarios.add(graph("second excluded intersection node", "CREATE " +
            "(n0)-[:REL]->(n1), (n1)-[:REL]->(n2), (n2)-[:REL]->(n0), " +
            "(n1)-[:REL]->(n3), (n3)-[:REL]->(n0), (n3)-[:REL]->(n4), " +
            "(n3)-[:REL]->(n5), (n3)-[:REL]->(n6)", 3));
        scenarios.add(graph("many triangles and other things", "CREATE " +
            "(a)-[:T]->(b)-[:T]->(b)-[:T]->(c)-[:T]->(a), " +
            "(c)-[:T]->(d)-[:T]->(e)-[:T]->(f)-[:T]->(d), " +
            "(f)-[:T]->(g)-[:T]->(h)-[:T]->(f), " +
            "(h)-[:T]->(i)-[:T]->(j)-[:T]->(k)-[:T]->(e), " +
            "(k)-[:T]->(l), (k)-[:T]->(m)-[:T]->(n)-[:T]->(j), (o)"));

        var maxDegreeGraph = "CREATE " +
            "(a)-[:T]->(b), (a)-[:T]->(c), (a)-[:T]->(d), " +
            "(b)-[:T]->(c), (b)-[:T]->(d), " +
            "(e)-[:T]->(f), (f)-[:T]->(g), (g)-[:T]->(e)";
        scenarios.add(graph("maximum degree", maxDegreeGraph, 2));
        scenarios.add(graph("maximum degree union", "CREATE " +
            "(a)-[:T1]->(b), (a)-[:T2]->(c), (a)-[:T2]->(d), " +
            "(b)-[:T1]->(c), (b)-[:T2]->(d), " +
            "(e)-[:T1]->(f), (f)-[:T1]->(g), (g)-[:T1]->(e)", 2));
        scenarios.add(graph("incomplete union triangle", "CREATE " +
            "(a)-[:T]->(b), (b)-[:X]->(x), (b)-[:Y]->(y), (a)-[:T]->(c)"));

        // Centrality also needs to distinguish one triangle neighbor from several triangles on that edge.
        scenarios.add(graph("shared triangle edge and nontriangle neighbor", SHARED_EDGE_GDL));
        return scenarios.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("triangleCountScenarios")
    void matchesTriangleCountAndNeighborTotals(Scenario scenario) {
        var graph = GdlSupport.fromGdl(scenario.gdl(), Orientation.UNDIRECTED).graph();
        var concurrency = new Concurrency(4);
        var triangleCount = IntersectingTriangleCount.create(
            graph, concurrency, scenario.maxDegree(), scenario.labelFilter(), DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE
        ).compute();
        var centrality = TriangleCentrality.create(
            graph, concurrency, scenario.maxDegree(), scenario.labelFilter(), DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(centrality.globalTriangleCount()).isEqualTo(triangleCount.globalTriangles());
        for (long node = 0; node < graph.nodeCount(); node++) {
            long count = triangleCount.localTriangles().get(node);
            assertThat(centrality.triangleCounts().get(node)).as("triangle count node %s", node)
                .isEqualTo(count);
            assertNeighborMetrics(graph, scenario.maxDegree(), triangleCount, centrality, node);

            long triangleSum = centrality.triangleNeighborsTriangleSums().get(node);
            long allSum = centrality.allNeighborsTriangleSums().get(node);

            if (count == EXCLUDED_NODE_TRIANGLE_COUNT) {
                assertThat(centrality.centralities().get(node)).as("centrality node %s", node).isNaN();
            } else {
                double expected = triangleCount.globalTriangles() == 0 ? 0.0
                    : ((count + triangleSum) / 3.0 + allSum - triangleSum) / triangleCount.globalTriangles();
                assertThat(centrality.centralities().get(node)).as("centrality node %s", node)
                    .isCloseTo(expected, org.assertj.core.data.Offset.offset(1e-10))
                    .isBetween(0.0, 1.0);
            }
        }
    }

    @Test
    void countsSharedTriangleEdgeOnceAsANeighbor() {
        // Edge a-b belongs to two triangles but contributes b's count to a only once. Leaf e
        // also receives a's count, at full weight, despite belonging to no triangle itself.
        var graph = GdlSupport.fromGdl(SHARED_EDGE_GDL, Orientation.UNDIRECTED);
        var centrality = TriangleCentrality.create(
            graph.graph(), new Concurrency(4), Long.MAX_VALUE, DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(centrality.globalTriangleCount()).isEqualTo(2);
        long a = graph.toMappedNodeId("a");
        long b = graph.toMappedNodeId("b");
        long c = graph.toMappedNodeId("c");
        long e = graph.toMappedNodeId("e");
        assertThat(centrality.triangleCounts().get(a)).isEqualTo(2);
        assertThat(centrality.triangleNeighbors().get(a)).isEqualTo(3);
        assertThat(centrality.allNeighbors().get(a)).isEqualTo(4);
        assertThat(centrality.triangleNeighborsTriangleSums().get(a)).isEqualTo(4);
        assertThat(centrality.allNeighborsTriangleSums().get(a)).isEqualTo(4);
        assertThat(centrality.centralities().get(a)).isEqualTo(1.0);
        assertThat(centrality.triangleNeighbors().get(b)).isEqualTo(3);
        assertThat(centrality.triangleNeighbors().get(c)).isEqualTo(2);
        assertThat(centrality.centralities().get(c)).isCloseTo(5.0 / 6.0,
            org.assertj.core.data.Offset.offset(1e-10));
        assertThat(centrality.triangleNeighbors().get(e)).isZero();
        assertThat(centrality.allNeighborsTriangleSums().get(e)).isEqualTo(2);
        assertThat(centrality.centralities().get(e)).isEqualTo(1.0);
    }

    @Test
    void labelFilterChangesCountsButKeepsTriangleNeighbors() {
        // The rejected a1-a4-a5 triangle still makes a1 a triangle neighbor of a4. Its accepted
        // count of 1 contributes 1/3 to a4; forgetting that edge mark would inflate the score to 1.
        var graph = GdlSupport.fromGdl(LABEL_FILTER_GDL, Orientation.UNDIRECTED);
        var centrality = TriangleCentrality.create(
            graph.graph(), new Concurrency(4), Long.MAX_VALUE, List.of("A", "B", "C"),
            DefaultPool.INSTANCE, ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE
        ).compute();

        long a1 = graph.toMappedNodeId("a1");
        long a4 = graph.toMappedNodeId("a4");
        assertThat(centrality.globalTriangleCount()).isEqualTo(1);
        assertThat(centrality.triangleNeighbors().get(a1)).isEqualTo(4);
        assertThat(centrality.allNeighbors().get(a1)).isEqualTo(4);
        assertThat(centrality.triangleNeighborsTriangleSums().get(a1)).isEqualTo(2);
        assertThat(centrality.allNeighborsTriangleSums().get(a1)).isEqualTo(2);
        assertThat(centrality.triangleNeighbors().get(a4)).isEqualTo(2);
        assertThat(centrality.allNeighbors().get(a4)).isEqualTo(2);
        assertThat(centrality.triangleNeighborsTriangleSums().get(a4)).isEqualTo(1);
        assertThat(centrality.allNeighborsTriangleSums().get(a4)).isEqualTo(1);
        assertThat(centrality.centralities().get(a4)).isEqualTo(1.0 / 3.0);
    }

    private static void assertNeighborMetrics(
        Graph graph, long maxDegree,
        TriangleCountResult triangleCount, TriangleCentralityResult centrality, long node
    ) {
        // Derive neighborhoods independently with sets, so the oracle cannot repeat a cursor-position
        // or parallel-edge deduplication mistake made by the algorithm's bitmap implementation.
        var neighbors = eligibleNeighbors(graph, maxDegree, node);
        var triangleNeighbors = new HashSet<Long>();
        for (long neighbor : neighbors) {
            var neighborNeighbors = eligibleNeighbors(graph, maxDegree, neighbor);
            for (long third : neighbors) {
                if (third != neighbor && neighborNeighbors.contains(third)) {
                    triangleNeighbors.add(neighbor);
                    break;
                }
            }
        }
        long triangleNeighborSum = triangleNeighbors.stream()
            .mapToLong(target -> triangleCount.localTriangles().get(target)).sum();
        long neighborSum = neighbors.stream().mapToLong(target -> triangleCount.localTriangles().get(target)).sum();
        assertThat(centrality.triangleNeighbors().get(node)).as("triangle neighbors node %s", node)
            .isEqualTo(triangleNeighbors.size());
        assertThat(centrality.allNeighbors().get(node)).as("all neighbors node %s", node)
            .isEqualTo(neighbors.size());
        assertThat(centrality.triangleNeighborsTriangleSums().get(node))
            .as("triangle neighbor sum node %s", node).isEqualTo(triangleNeighborSum);
        assertThat(centrality.allNeighborsTriangleSums().get(node)).as("all neighbor sum node %s", node)
            .isEqualTo(neighborSum);
    }

    private static Set<Long> eligibleNeighbors(Graph graph, long maxDegree, long node) {
        var neighbors = new HashSet<Long>();
        if (graph.degree(node) <= maxDegree) {
            graph.forEachRelationship(node, (source, target) -> {
                if (target != source && graph.degree(target) <= maxDegree) {
                    neighbors.add(target);
                }
                return true;
            });
        }
        return neighbors;
    }

    private static Scenario graph(String name, String gdl) {
        return new Scenario(name, gdl, Long.MAX_VALUE, List.of());
    }

    private static Scenario graph(String name, String gdl, long maxDegree) {
        return new Scenario(name, gdl, maxDegree, List.of());
    }

    private static Scenario graph(String name, String gdl, List<String> labels) {
        return new Scenario(name, gdl, Long.MAX_VALUE, labels);
    }

    private record Scenario(String name, String gdl, long maxDegree, List<String> labelFilter) {
        @Override
        public String toString() {
            return name;
        }
    }
}
