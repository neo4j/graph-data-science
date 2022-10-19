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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.gds.ml.negativeSampling.NegativeSampler.NEGATIVE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class DirectedEdgeSplitterTest extends EdgeSplitterBaseTest {

    @GdlGraph
    static String gdl =
        "(a1:A), " +
        "(a2:A), " +
        "(a3:A), " +
        "(a4:A), " +
        "(a5:A), " +
        "(a6:A), " +
        "(a1)-[:T {foo: 5} ]->(a2), " +
        "(a2)-[:T {foo: 5} ]->(a3), " +
        "(a3)-[:T {foo: 5} ]->(a4), " +
        "(a4)-[:T {foo: 5} ]->(a5), " +
        "(a5)-[:T {foo: 5} ]->(a6)";

    @Inject
    GraphStore graphStore;

    @Inject
    TestGraph graph;

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel =
        "(n1 :A), " +
        "(n2 :C), " +
        "(n3 :A), " +
        "(n4 :A), " +
        "(n5 :B), " +
        "(n6 :D), " +
        "(n1)-[:T {foo: 5} ]->(n2), " +
        "(n2)-[:T {foo: 5} ]->(n3), " +
        "(n3)-[:T {foo: 5} ]->(n4), " +
        "(n4)-[:T {foo: 5} ]->(n5), " +
        "(n5)-[:T {foo: 5} ]->(n6)";

    @Inject
    TestGraph multiLabelGraph;

    @Inject
    GraphStore multiLabelGraphStore;

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "multi")
    static String gdlMultiGraph =
        "(n1:A), " +
        "(n2:A), " +
        "(n1)-->(n2), " +
        "(n1)-->(n2), " +
        "(n1)-->(n2), " +
        "(n1)-->(n2)";

    @Inject
    TestGraph multiGraph;

    @Inject
    GraphStore multiGraphStore;

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "skewed")
    static String gdlSkewed =
        "(a1:A), " +
        "(a2:A), " +
        "(a3:A), " +
        "(a4:A), " +
        "(a5:A), " +
        "(a6:A), " +
        "(a1)-[:T {foo: 5} ]->(a2), " +
        "(a1)-[:T {foo: 5} ]->(a3), " +
        "(a1)-[:T {foo: 5} ]->(a4), " +
        "(a1)-[:T {foo: 5} ]->(a5), " +
        "(a1)-[:T {foo: 5} ]->(a6)";

    @Inject
    GraphStore skewedGraphStore;

    @Inject
    TestGraph skewedGraph;

    @Test
    void splitSkewedGraph() {
        var splitter = new DirectedEdgeSplitter(
            Optional.of(-1L),
            skewedGraphStore.nodes(),
            skewedGraphStore.nodes(),
            4
        );

        EdgeSplitter.SplitResult result = splitter.splitPositiveExamples(skewedGraph, 1.0);

        assertThat(result.selectedRelCount()).isEqualTo(5);
        assertThat(result.selectedRels().build().topology().elementCount()).isEqualTo(5);
        assertThat(result.remainingRelCount()).isEqualTo(0);
        assertThat(result.remainingRels().build().topology().elementCount()).isEqualTo(0);
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
            .matches(topology -> topology.elementCount() == 1);
    }


    @Test
    void split() {
        var splitter = new DirectedEdgeSplitter(
            Optional.of(-1L),
            graphStore.nodes(),
            graphStore.nodes(),
            4
        );

        // select 40%, which is 2 rels in this graph
        var result = splitter.splitPositiveExamples(graph, .4);

        var remainingRels = result.remainingRels().build();

        // 2 positive selected reduces remaining
        assertEquals(3L, remainingRels.topology().elementCount());
        assertEquals(Orientation.NATURAL, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();
        assertRelInGraph(remainingRels, graph);

        var selectedRels = result.selectedRels().build();

        assertRelSamplingProperties(selectedRels, graph);
        assertThat(selectedRels.topology().elementCount()).isEqualTo(2);
        assertFalse(selectedRels.topology().isMultiGraph());
    }

    @Test
    void negativeEdgesShouldNotOverlapMasterGraph() {
        var huuuuugeDenseGraph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(95)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(123)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.NATURAL)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .build()
            .generate();

        var splitter = new DirectedEdgeSplitter(Optional.of(42L), huuuuugeDenseGraph, huuuuugeDenseGraph, 4);
        var splitResult = splitter.splitPositiveExamples(huuuuugeDenseGraph, 0.9);
        var graph = GraphFactory.create(
            huuuuugeDenseGraph.idMap(),
            splitResult.remainingRels().build(),
            Orientation.NATURAL
        );
        var nestedSplit = splitter.splitPositiveExamples(graph, 0.9);
        Relationships nestedHoldout = nestedSplit.selectedRels().build();
        HugeGraph nestedHoldoutGraph = GraphFactory.create(graph, nestedHoldout, Orientation.NATURAL);
        nestedHoldoutGraph.forEachNode(nodeId -> {
            nestedHoldoutGraph.forEachRelationship(nodeId, Double.NaN, (src, trg, val) -> {
                if (Double.compare(val, NEGATIVE) == 0) {
                    assertFalse(
                        huuuuugeDenseGraph.exists(src, trg),
                        formatWithLocale("Sampled negative edge %d,%d is an edge of the master graph.", src, trg)
                    );
                }
                return true;
            } );
            return true;
        });
    }

    @Test
    void negativeEdgeSampling() {
        var splitter = new DirectedEdgeSplitter(Optional.of(42L), graphStore.nodes(), graphStore.nodes(), 4);

        var sum = 0;
        for (int i = 0; i < 100; i++) {
            var prev = splitter.samplesPerNode(i, 1000 - sum, 100 - i);
            sum += prev;
        }

        assertEquals(1000, sum);
    }

    @Test
    void splitWithFilteringWithDifferentSourceTargetLabels() {
        Collection<NodeLabel> sourceNodeLabels = List.of(NodeLabel.of("A"), NodeLabel.of("B"));
        Collection<NodeLabel> targetNodeLabels = List.of(NodeLabel.of("C"), NodeLabel.of("D"));
        var splitter = new DirectedEdgeSplitter(
            Optional.of(1337L),
            multiLabelGraphStore.getGraph(sourceNodeLabels),
            multiLabelGraphStore.getGraph(targetNodeLabels),
            4
        );

        // select 60%, which is 2*0.6 rounded down to 1 rel in this graph. 3 were invalid.
        var result = splitter.splitPositiveExamples(multiLabelGraph, .6);

        var remainingRels = result.remainingRels().build();
        // 1 positive selected reduces remaining & 2 invalid
        assertEquals(1L, remainingRels.topology().elementCount());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();
        assertRelInGraph(remainingRels, multiLabelGraph);

        var selectedRels = result.selectedRels().build();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            assertRelSamplingProperties(selectedRels, multiLabelGraph);
            assertThat(topology.elementCount()).isEqualTo(1);
            assertFalse(topology.isMultiGraph());
        });

        assertNodeLabelFilter(selectedRels.topology(), sourceNodeLabels, targetNodeLabels, multiLabelGraph);

    }

    @Test
    void samplesWithinBounds() {
        var splitter = new DirectedEdgeSplitter(Optional.of(42L), graphStore.nodes(), graphStore.nodes(), 4);

        assertEquals(1, splitter.samplesPerNode(1, 100, 10));
        assertEquals(1, splitter.samplesPerNode(100, 1, 1));
    }

    @Test
    void shouldPreserveRelationshipWeights() {
        var splitter = new DirectedEdgeSplitter(Optional.of(42L), graphStore.nodes(), graphStore.nodes(), 4);
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
}
