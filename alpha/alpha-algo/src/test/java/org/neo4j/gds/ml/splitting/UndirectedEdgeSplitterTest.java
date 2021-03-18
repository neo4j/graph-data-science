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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.NEGATIVE;
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.POSITIVE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdlExtension
class UndirectedEdgeSplitterTest {

    @GdlGraph(orientation = Orientation.UNDIRECTED)
    static String gdl = "(:A)-[:T]->(:A)-[:T]->(:A)-[:T]->(:A)-[:T]->(:A)-[:T]->(:A)";

    @Inject
    TestGraph graph;

    @Test
    void split() {
        var splitter = new UndirectedEdgeSplitter(1337L);

        // select 20%, which is 1 (undirected) rels in this graph
        var result = splitter.split(graph, .2);

        var remainingRels = result.remainingRels();
        // 1 positive selected reduces remaining
        assertEquals(8L, remainingRels.topology().elementCount());
        assertEquals(Orientation.UNDIRECTED, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isEmpty();

        var selectedRels = result.selectedRels();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            // it selected 2,5 (neg) and 3,2 (pos) relationships
            assertEquals(2L, topology.elementCount());
            var cursor = topology.list().decompressingCursor(topology.offsets().get(3L), topology.degrees().degree(3L));
            assertEquals(2L, cursor.nextVLong());
            var cursor2 = topology.list().decompressingCursor(topology.offsets().get(2L), topology.degrees().degree(2L));
            assertEquals(5L, cursor2.nextVLong());
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });
        assertThat(selectedRels.properties()).isPresent().get().satisfies(p -> {
            assertEquals(2L, p.elementCount());
            var cursor = p.list().cursor(p.offsets().get(3L), p.degrees().degree(3L));
            // 3,2 is positive
            assertEquals(POSITIVE, Double.longBitsToDouble(cursor.nextLong()));
            var cursor2 = p.list().cursor(p.offsets().get(2L), p.degrees().degree(2L));
            // 2,5 is negative
            assertEquals(NEGATIVE, Double.longBitsToDouble(cursor2.nextLong()));
        });
    }

    @Test
    void negativeEdgesShouldNotOverlapMasterGraph() {
        var huuuuugeDenseGraph = new RandomGraphGenerator(
            100,
            95,
            RelationshipDistribution.UNIFORM,
            123L,
            Optional.empty(),
            Map.of(),
            Optional.empty(),
            Aggregation.SINGLE,
            Orientation.UNDIRECTED,
            RandomGraphGeneratorConfig.AllowSelfLoops.NO,
            AllocationTracker.empty()
        ).generate();

        var splitter = new UndirectedEdgeSplitter(42L);
        var splitResult = splitter.split(huuuuugeDenseGraph, 0.9);
        var graph = GraphFactory.create(
            huuuuugeDenseGraph.idMap(),
            splitResult.remainingRels(),
            AllocationTracker.empty()
        );
        var nestedSplit = splitter.split(graph, huuuuugeDenseGraph, 0.9);
        Relationships nestedHoldout = nestedSplit.selectedRels();
        HugeGraph nestedHoldoutGraph = GraphFactory.create(graph, nestedHoldout, AllocationTracker.empty());
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
    void shouldProduceDeterministicResult() {
        var graph = new RandomGraphGenerator(
            100,
            95,
            RelationshipDistribution.UNIFORM,
            123L,
            Optional.empty(),
            Map.of(),
            Optional.empty(),
            Aggregation.SINGLE,
            Orientation.UNDIRECTED,
            RandomGraphGeneratorConfig.AllowSelfLoops.NO,
            AllocationTracker.empty()
        ).generate();

        var splitResult1 = new UndirectedEdgeSplitter(12L).split(graph, 0.5);
        var splitResult2 = new UndirectedEdgeSplitter(12L).split(graph, 0.5);
        var remainingAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.remainingRels(),
            splitResult2.remainingRels()
        );
        assertTrue(remainingAreEqual);

        var holdoutAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.selectedRels(),
            splitResult2.selectedRels()
        );
        assertTrue(holdoutAreEqual);
    }

    @Test
    void shouldProduceNonDeterministicResult() {
        var graph = new RandomGraphGenerator(
            100,
            95,
            RelationshipDistribution.UNIFORM,
            123L,
            Optional.empty(),
            Map.of(),
            Optional.empty(),
            Aggregation.SINGLE,
            Orientation.UNDIRECTED,
            RandomGraphGeneratorConfig.AllowSelfLoops.NO,
            AllocationTracker.empty()
        ).generate();

        var splitResult1 = new UndirectedEdgeSplitter(Optional.empty()).split(graph, 0.5);
        var splitResult2 = new UndirectedEdgeSplitter(Optional.empty()).split(graph, 0.5);
        var remainingAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.remainingRels(),
            splitResult2.remainingRels()
        );
        assertFalse(remainingAreEqual);

        var holdoutAreEqual = relationshipsAreEqual(
            graph,
            splitResult1.selectedRels(),
            splitResult2.selectedRels()
        );
        assertFalse(holdoutAreEqual);
    }

    @Test
    void negativeEdgeSampling() {
        var splitter = new UndirectedEdgeSplitter(42L);

        var sum = 0;
        for (int i = 0; i < 100; i++) {
            var prev = splitter.samplesPerNode(i, 1000 - sum, 100 - i);
            sum += prev;
        }

        assertEquals(1000, sum);
    }

    @Test
    void samplesWithinBounds() {
        var splitter = new UndirectedEdgeSplitter(42L);

        assertEquals(1, splitter.samplesPerNode(1, 100, 10));
        assertEquals(1, splitter.samplesPerNode(100, 1, 1));
    }

    private boolean relationshipsAreEqual(NodeMapping mapping, Relationships r1, Relationships r2) {
        var fallbackValue = -0.66;
        if (r1.topology().elementCount() != r2.topology().elementCount()) {
            return false;
        }
        var g1 = GraphFactory.create(mapping, r1, AllocationTracker.empty());
        var g2 = GraphFactory.create(mapping, r2, AllocationTracker.empty());
        var equalSoFar = new AtomicBoolean(true);
        g1.forEachNode(nodeId -> {
            g1.forEachRelationship(nodeId, fallbackValue, (source, target, val) -> {
                var g2Property = g2.relationshipProperty(source, target, fallbackValue);
                if ((!g2.exists(source, target)) || (Double.compare(g2Property, val) != 0))  {
                    equalSoFar.set(false);
                }
                return equalSoFar.get();
            });
            return equalSoFar.get();
        });
        return equalSoFar.get();
    }
}
