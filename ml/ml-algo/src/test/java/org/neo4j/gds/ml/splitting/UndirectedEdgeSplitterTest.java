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
package org.neo4j.gds.ml.splitting;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyCursor;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.NEGATIVE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class UndirectedEdgeSplitterTest extends EdgeSplitterBaseTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String gdl = "(n1 :A)-[:T {foo: 5} ]->(n2 :A)-[:T {foo: 5} ]->(n3 :A)-[:T {foo: 5} ]->(n4 :A)-[:T {foo: 5} ]->(n5 :B)-[:T {foo: 5} ]->(n6 :A)";

    @Inject
    TestGraph graph;

    @Inject
    GraphStore graphStore;

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel = "(n1 :A)-[:T {foo: 5} ]->(n2 :C)-[:T {foo: 5} ]->(n3 :A)-[:T {foo: 5} ]->(n4 :A)-[:T {foo: 5} ]->(n5 :B)-[:T {foo: 5} ]->(n6 :D)";

    @Inject
    TestGraph multiLabelGraph;

    @Inject
    GraphStore multiLabelGraphStore;

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "multi")
    static String gdlMultiGraph = "(n1 :A), (n2: A), (n1)-->(n2), (n2)-->(n1), (n1)-->(n2), (n1)-->(n2)";

    @Inject
    TestGraph multiGraph;

    @Inject
    GraphStore multiGraphStore;

    @GdlGraph(orientation = Orientation.UNDIRECTED, graphNamePrefix = "negative")
    static String gdlNegative = "(n1 :A)-[:T {foo: 5} ]->(n2 :A)-[:T {foo: 5} ]->(n3 :A)-[:T {foo: 5} ]->(n4 :A)-[:T {foo: 5} ]->(n5 :B)-[:T {foo: 5} ]->(n6 :A), (n1)-[:NEGATIVE]->(n3), (n3)-[:NEGATIVE]->(n5), (n5)-[:NEGATIVE]->(n7 :A)";

    @Inject
    TestGraph negativeGraph;

    @Inject
    GraphStore negativeGraphStore;

    @Test
    void split() {
        double negativeSamplingRatio = 1.0;
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(1337L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        );

        // select 20%, which is 1 (undirected) rels in this graph
        var result = splitter.splitPositiveExamples(graph, .2);

        var remainingRels = result.remainingRels().build();
        // 1 positive selected reduces remaining
        assertEquals(8L, remainingRels.topology().elementCount());
        assertEquals(Orientation.UNDIRECTED, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();

        var selectedRels = result.selectedRels().build();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            assertRelSamplingProperties(selectedRels, graph);
            assertThat(topology.elementCount()).isEqualTo(1);
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });
    }

    @Test
    void splitMultiGraph() {
        var splitter = new DirectedEdgeSplitter(
            Optional.of(-1L),
            multiGraphStore.nodes(),
            multiGraphStore.nodes(),
            4
        );

        EdgeSplitter.SplitResult result = splitter.splitPositiveExamples(multiGraph, 0.5);

        assertThat(result.selectedRels().build().topology())
            // we always aggregate the result at the moment
            .matches(topology -> !topology.isMultiGraph())
            .matches(topology -> topology.elementCount() == 2);
    }

    @Test
    void negativeEdgesShouldNotOverlapMasterGraph() {
        var huuuuugeDenseGraph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(95)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        var splitter = new UndirectedEdgeSplitter(
            Optional.of(42L),
            huuuuugeDenseGraph,
            huuuuugeDenseGraph,
            4
        );
        var splitResult = splitter.splitPositiveExamples(huuuuugeDenseGraph, 0.9);
        var graph = GraphFactory.create(
            huuuuugeDenseGraph.idMap(),
            splitResult.remainingRels().build()
        );
        var nestedSplit = splitter.splitPositiveExamples(graph, 0.9);
        Relationships nestedHoldout = nestedSplit.selectedRels().build();
        HugeGraph nestedHoldoutGraph = GraphFactory.create(graph, nestedHoldout);
        nestedHoldoutGraph.forEachNode(nodeId -> {
            nestedHoldoutGraph.forEachRelationship(nodeId, Double.NaN, (src, trg, val) -> {
                if (Double.compare(val, NEGATIVE) == 0) {
                    assertFalse(
                        huuuuugeDenseGraph.exists(src, trg),
                        formatWithLocale("Sampled negative edge %d,%d is an edge of the master graph.", src, trg)
                    );
                }
                return true;
            });
            return true;
        });
    }

    @Test
    void shouldProduceDeterministicResult() {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(95)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        var splitResult1 = new UndirectedEdgeSplitter(
            Optional.of(12L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        ).splitPositiveExamples(graph, 0.5);
        var splitResult2 = new UndirectedEdgeSplitter(
            Optional.of(12L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        ).splitPositiveExamples(graph, 0.5);
        var remainingAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.remainingRels().build(),
            splitResult2.remainingRels().build()
        );
        assertTrue(remainingAreEqual);

        var holdoutAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.selectedRels().build(),
            splitResult2.selectedRels().build()
        );
        assertTrue(holdoutAreEqual);
    }

    @Test
    void shouldProduceNonDeterministicResult() {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(95)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        var splitResult1 = new UndirectedEdgeSplitter(
            Optional.of(42L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        ).splitPositiveExamples(graph, 0.5);
        var splitResult2 = new UndirectedEdgeSplitter(
            Optional.of(117L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        ).splitPositiveExamples(graph, 0.5);
        var remainingAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.remainingRels().build(),
            splitResult2.remainingRels().build()
        );
        assertFalse(remainingAreEqual);

        var holdoutAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.selectedRels().build(),
            splitResult2.selectedRels().build()
        );
        assertFalse(holdoutAreEqual);
    }

    @Test
    void negativeEdgeSampling() {
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(42L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        );

        var sum = 0;
        for (int i = 0; i < 100; i++) {
            var prev = splitter.samplesPerNode(i, 1000 - sum, 100 - i);
            sum += prev;
        }

        assertEquals(1000, sum);
    }

    @Test
    void splitWithFilteringWithSameSourceTargetLabels() {
        var sourceNodeLabels = List.of(NodeLabel.of("A"));
        var targetNodeLabels = List.of(NodeLabel.of("A"));
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(1337L),
            graphStore.getGraph(NodeLabel.of("A")),
            graphStore.getGraph(NodeLabel.of("A")),
            4
        );

        // select 40%, which is 1.2 rounded down to 1 (undirected) rels in this graph
        var result = splitter.splitPositiveExamples(graph, .4);

        var remainingRels = result.remainingRels().build();
        // 1 positive selected reduces remaining & 4 invalid relationships
        assertEquals(4L, remainingRels.topology().elementCount());
        assertEquals(Orientation.UNDIRECTED, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();

        var selectedRels = result.selectedRels().build();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            assertRelSamplingProperties(selectedRels, graph);
            assertThat(topology.elementCount()).isEqualTo(1);
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });

        assertNodeLabelFilter(selectedRels.topology(), sourceNodeLabels, targetNodeLabels, graph);

    }

    @Test
    void splitWithFilteringWithDifferentSourceTargetLabels() {
        Collection<NodeLabel> sourceNodeLabels = List.of(NodeLabel.of("A"), NodeLabel.of("B"));
        Collection<NodeLabel> targetNodeLabels = List.of(NodeLabel.of("C"), NodeLabel.of("D"));
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(1337L),
            multiLabelGraphStore.getGraph(sourceNodeLabels),
            multiLabelGraphStore.getGraph(targetNodeLabels),
            4
        );

        // select 70%, which is 6*0.7/2 rounded down to 2 (undirected) rels in this graph. 4 were invalid.
        var result = splitter.splitPositiveExamples(multiLabelGraph, .7);

        var remainingRels = result.remainingRels().build();
        // 2 positive selected reduces remaining & 4 invalid relationships
        assertEquals(2L, remainingRels.topology().elementCount());
        assertEquals(Orientation.UNDIRECTED, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();
        assertRelInGraph(remainingRels, multiLabelGraph);

        var selectedRels = result.selectedRels().build();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            assertRelSamplingProperties(selectedRels, multiLabelGraph);
            assertThat(topology.elementCount()).isEqualTo(2);
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });

        assertNodeLabelFilter(selectedRels.topology(), sourceNodeLabels, targetNodeLabels, multiLabelGraph);
    }

    @Test
    void samplesWithinBounds() {
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(42L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        );

        assertEquals(1, splitter.samplesPerNode(1, 100, 10));
        assertEquals(1, splitter.samplesPerNode(100, 1, 1));
    }

    @Test
    void shouldPreserveRelationshipWeights() {
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(42L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        );
        EdgeSplitter.SplitResult split = splitter.splitPositiveExamples(graph, 0.01);
        var maybeProp = split.remainingRels().build().properties();
        assertThat(maybeProp).isPresent();
        graph.forEachNode(nodeId -> {
            PropertyCursor propertyCursor = maybeProp.get().propertiesList().propertyCursor(nodeId);
            while (propertyCursor.hasNextLong()) {
                assertThat(Double.longBitsToDouble(propertyCursor.nextLong())).isEqualTo(5.0);
            }
            return true;
        });
    }

    @Test
    void zeroNegativeSamples() {
        var splitter = new UndirectedEdgeSplitter(
            Optional.of(1337L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        );

        // select 20%, which is 1 (undirected) rels in this graph
        var result = splitter.splitPositiveExamples(graph, .2);

        assertRelSamplingProperties(result.selectedRels().build(), graph);
        assertThat(result.selectedRels().build().topology().elementCount()).isEqualTo(1);
    }

    private boolean relationshipsAreEqual(IdMap mapping, Relationships r1, Relationships r2) {
        var fallbackValue = -0.66;
        if (r1.topology().elementCount() != r2.topology().elementCount()) {
            return false;
        }
        var g1 = GraphFactory.create(mapping, r1);
        var g2 = GraphFactory.create(mapping, r2);
        var equalSoFar = new AtomicBoolean(true);
        g1.forEachNode(nodeId -> {
            g1.forEachRelationship(nodeId, fallbackValue, (source, target, val) -> {
                var g2Property = g2.relationshipProperty(source, target, fallbackValue);
                if ((!g2.exists(source, target)) || (Double.compare(g2Property, val) != 0)) {
                    equalSoFar.set(false);
                }
                return equalSoFar.get();
            });
            return equalSoFar.get();
        });
        return equalSoFar.get();
    }
}
