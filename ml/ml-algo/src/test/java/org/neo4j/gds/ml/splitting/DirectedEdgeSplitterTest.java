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
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.NEGATIVE;
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.POSITIVE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class DirectedEdgeSplitterTest extends EdgeSplitterBaseTest {

    @GdlGraph
    static String gdl = "(:A)-[:T {foo: 5} ]->(:A)-[:T {foo: 5} ]->(:A)-[:T {foo: 5} ]->(:A)-[:T {foo: 5} ]->(:A)-[:T {foo: 5} ]->(:A)";

    @Inject
    TestGraph graph;

    @GdlGraph(orientation = Orientation.NATURAL, graphNamePrefix = "multiLabel")
    static String gdlMultiLabel = "(n1 :A)-[:T {foo: 5} ]->(n2 :C)-[:T {foo: 5} ]->(n3 :A)-[:T {foo: 5} ]->(n4 :A)-[:T {foo: 5} ]->(n5 :B)-[:T {foo: 5} ]->(n6 :D)";

    @Inject
    TestGraph multiLabelGraph;


    @Test
    void split() {
        var splitter = new DirectedEdgeSplitter(Optional.of(-1L), 1.0, graph.availableNodeLabels(), graph.availableNodeLabels(), 4);

        // select 40%, which is 2 rels in this graph
        var result = splitter.split(graph, .4);

        var remainingRels = result.remainingRels();

        // 2 positive selected reduces remaining
        assertEquals(3L, remainingRels.topology().elementCount());
        assertEquals(Orientation.NATURAL, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();

        var selectedRels = result.selectedRels();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            // it selected 4,0 and 5,4 (neg) and 0,1 and 2,3 (pos) relationships
            assertEquals(4L, topology.elementCount());
            assertRelExists(topology, 4, 0);
            assertRelExists(topology, 5, 4);
            assertRelExists(topology, 0, 1);
            assertRelExists(topology, 2, 3);
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });
        assertThat(selectedRels.properties()).isPresent().get().satisfies(p -> {
            assertEquals(4L, p.elementCount());
            assertRelProperties(p, 4, NEGATIVE);
            assertRelProperties(p, 5, NEGATIVE);
            assertRelProperties(p, 0, POSITIVE);
            assertRelProperties(p, 2, POSITIVE);
        });
    }

    @Test
    void splitWithNegativeRatio() {
        var splitter = new DirectedEdgeSplitter(Optional.of(-1L), 2.0, graph.availableNodeLabels(), graph.availableNodeLabels(), 4);

        // select 40%, which is 2 rels in this graph
        var result = splitter.split(graph, .4);

        var remainingRels = result.remainingRels();

        // 2 positive selected reduces remaining
        assertEquals(3L, remainingRels.topology().elementCount());
        assertEquals(Orientation.NATURAL, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();

        var selectedRels = result.selectedRels();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            // it selected 0,5; 3,0; 4,2; 5,3 (neg) and 0,1 and 1,2 (pos) relationships
            assertEquals(6L, topology.elementCount());
            assertRelExists(topology, 0, 1, 5);
            assertRelExists(topology, 3, 0);
            assertRelExists(topology, 4, 2);
            assertRelExists(topology, 5, 3);
            assertRelExists(topology, 1, 2);
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });
        assertThat(selectedRels.properties()).isPresent().get().satisfies(p -> {
            assertEquals(6L, p.elementCount());
            assertRelProperties(p, 0, POSITIVE, NEGATIVE);
            assertRelProperties(p, 3, NEGATIVE);
            assertRelProperties(p, 4, NEGATIVE);
            assertRelProperties(p, 5, NEGATIVE);
            assertRelProperties(p, 1, POSITIVE);
        });
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

        var splitter = new DirectedEdgeSplitter(Optional.of(42L), 1.0, huuuuugeDenseGraph.availableNodeLabels(), huuuuugeDenseGraph.availableNodeLabels(), 4);
        var splitResult = splitter.split(huuuuugeDenseGraph, 0.9);
        var graph = GraphFactory.create(
            huuuuugeDenseGraph.idMap(),
            splitResult.remainingRels()
        );
        var nestedSplit = splitter.split(graph, huuuuugeDenseGraph, 0.9);
        Relationships nestedHoldout = nestedSplit.selectedRels();
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
            } );
            return true;
        });
    }

    @Test
    void negativeEdgeSampling() {
        var splitter = new DirectedEdgeSplitter(Optional.of(42L), 1.0, graph.availableNodeLabels(), graph.availableNodeLabels(), 4);

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
        double negativeSamplingRatio = 2.0;
        var splitter = new DirectedEdgeSplitter(Optional.of(1337L), negativeSamplingRatio, sourceNodeLabels, targetNodeLabels, 4);

        // select 60%, which is 2*0.6 rounded down to 1 rel in this graph. 3 were invalid.
        var result = splitter.split(multiLabelGraph, .6);

        var remainingRels = result.remainingRels();
        // 1 positive selected reduces remaining
        assertRelSamplingProperties(remainingRels, multiLabelGraph, negativeSamplingRatio);
        assertEquals(Orientation.NATURAL, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isNotEmpty();

        var selectedRels = result.selectedRels();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            assertRelSamplingProperties(selectedRels, multiLabelGraph, negativeSamplingRatio);
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });

        assertNodeLabelFilter(selectedRels.topology(), sourceNodeLabels, targetNodeLabels, multiLabelGraph);

    }

    @Test
    void samplesWithinBounds() {
        var splitter = new DirectedEdgeSplitter(Optional.of(42L), 1.0, graph.availableNodeLabels(), graph.availableNodeLabels(), 4);

        assertEquals(1, splitter.samplesPerNode(1, 100, 10));
        assertEquals(1, splitter.samplesPerNode(100, 1, 1));
    }

    @Test
    void shouldPreserveRelationshipWeights() {
        var splitter = new DirectedEdgeSplitter(Optional.of(42L), 1.0, graph.availableNodeLabels(), graph.availableNodeLabels(), 4);
        EdgeSplitter.SplitResult split = splitter.split(graph, 0.01);
        var maybeProp = split.remainingRels().properties();
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
