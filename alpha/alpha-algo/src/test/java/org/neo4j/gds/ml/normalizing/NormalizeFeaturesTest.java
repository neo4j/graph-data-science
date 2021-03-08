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
package org.neo4j.gds.ml.normalizing;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@GdlExtension
class NormalizeFeaturesTest {

    @GdlGraph
    static String GDL =
        "(a:A {a: 1.1D}), " +
        "(b:A {a: 2.8D}), " +
        "(c:A {a: 3}), " +
        "(d:A {a: -1}), " +
        "(e:A {a: -10})";

    @Inject
    TestGraph graph;

    @Test
    void outputsAList() {
        var config = ImmutableNormalizeFeaturesConfig.builder().featureProperties(List.of("a")).build();
        var algo = new NormalizeFeatures(graph, config, AllocationTracker.empty());

        var result = algo.compute();

        var properties = result.normalizedProperties().toArray();
        assertArrayEquals(new double[]{1.1}, properties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{2.8}, properties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{3D}, properties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{-1D}, properties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{-10D}, properties[(int) graph.toOriginalNodeId("e")]);
    }

}