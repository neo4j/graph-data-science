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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.NEGATIVE;
import static org.neo4j.gds.ml.splitting.DirectedEdgeSplitter.POSITIVE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdlExtension
class DirectedEdgeSplitterTest {

    @GdlGraph
    static String gdl = "(:A)-[:T]->(:A)-[:T]->(:A)-[:T]->(:A)-[:T]->(:A)-[:T]->(:A)";

    @Inject
    TestGraph graph;

    @Test
    void split() {
        var splitter = new DirectedEdgeSplitter(3L);

        // select 40%, which is 2 rels in this graph
        var result = splitter.split(graph, .4);

        var remainingRels = result.remainingRels();

        // 2 positive selected reduces remaining
        assertEquals(3L, remainingRels.topology().elementCount());
        assertEquals(Orientation.NATURAL, remainingRels.topology().orientation());
        assertFalse(remainingRels.topology().isMultiGraph());
        assertThat(remainingRels.properties()).isEmpty();

        var selectedRels = result.selectedRels();
        assertThat(selectedRels.topology()).satisfies(topology -> {
            // it selected 5,0 and 5,3 (neg) and 1,2 and 2,3 (pos) relationships
            assertEquals(4L, topology.elementCount());
            var cursor = topology.list().decompressingCursor(topology.offsets().get(5L), topology.degrees().degree(5L));
            assertEquals(0L, cursor.nextVLong());
            assertEquals(3L, cursor.nextVLong());
            var cursor2 = topology.list().decompressingCursor(topology.offsets().get(1L), topology.degrees().degree(1L));
            assertEquals(2L, cursor2.nextVLong());
            var cursor3 = topology.list().decompressingCursor(topology.offsets().get(2L), topology.degrees().degree(2L));
            assertEquals(3L, cursor3.nextVLong());
            assertEquals(Orientation.NATURAL, topology.orientation());
            assertFalse(topology.isMultiGraph());
        });
        assertThat(selectedRels.properties()).isPresent().get().satisfies(p -> {
            assertEquals(4L, p.elementCount());
            var cursor = p.list().cursor(p.offsets().get(5L), p.degrees().degree(5L));
            // 5,0 is negative
            assertEquals(NEGATIVE, Double.longBitsToDouble(cursor.nextLong()));
            // 5,3 is positive
            assertEquals(NEGATIVE, Double.longBitsToDouble(cursor.nextLong()));
            var cursor2 = p.list().cursor(p.offsets().get(1L), p.degrees().degree(1L));
            // 1,2 is positive
            assertEquals(POSITIVE, Double.longBitsToDouble(cursor2.nextLong()));
            var cursor3 = p.list().cursor(p.offsets().get(2L), p.degrees().degree(2L));
            // 2,3 is positive
            assertEquals(POSITIVE, Double.longBitsToDouble(cursor3.nextLong()));
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
            Orientation.NATURAL,
            RandomGraphGeneratorConfig.AllowSelfLoops.NO,
            AllocationTracker.empty()
        ).generate();

        var splitter = new DirectedEdgeSplitter(42L);
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
    void negativeEdgeSampling() {
        var splitter = new DirectedEdgeSplitter(42L);

        var sum = 0;
        for (int i = 0; i < 100; i++) {
            var prev = splitter.samplesPerNode(i, 1000 - sum, 100 - i);
            sum += prev;
        }

        assertEquals(1000, sum);
    }

    @Test
    void samplesWithinBounds() {
        var splitter = new DirectedEdgeSplitter(42L);

        assertEquals(1, splitter.samplesPerNode(1, 100, 10));
        assertEquals(1, splitter.samplesPerNode(100, 1, 1));
    }

}
