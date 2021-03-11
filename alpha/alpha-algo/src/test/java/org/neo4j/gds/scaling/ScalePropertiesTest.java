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
package org.neo4j.gds.scaling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@GdlExtension
class ScalePropertiesTest {

    @GdlGraph
    static String GDL =
        "(a:A {a: 1.1D, b: 20, c: 50}), " +
        "(b:A {a: 2.8D, b: 21, c: 51}), " +
        "(c:A {a: 3, b: 22, c: 52}), " +
        "(d:A {a: -1, b: 23, c: 60}), " +
        "(e:A {a: -10, b: 24, c: 100})";

    @Inject
    TestGraph graph;

    @ParameterizedTest
    @ValueSource(ints = {1,4})
    void minmaxNormalisation(int concurrency) {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .featureProperties(List.of("a"))
            .scalers(List.of("MinMax"))
            .concurrency(concurrency)
            .build();
        var algo = new ScaleProperties(graph, config, AllocationTracker.empty());

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1/13D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8/13D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9/13D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    @ParameterizedTest
    @ValueSource(ints = {1,4})
    void minmaxNormalisationOverMultipleProperties(int concurrency) {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .featureProperties(List.of("a", "b", "c"))
            .scalers(List.of("MinMax", "MinMax", "MinMax"))
            .concurrency(concurrency)
            .build();
        var algo = new ScaleProperties(graph, config, AllocationTracker.empty());

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1/13D, 0D, 0D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8/13D, 0.25, 1/50D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D, 0.5, 2/50D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9/13D, 0.75, 10/50D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D, 1D, 1D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    @ParameterizedTest
    @ValueSource(ints = {1,4})
    void differentNormalizers(int concurrency) {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .featureProperties(List.of("a", "b"))
            .scalers(List.of("MinMax", "Mean"))
            .concurrency(concurrency)
            .build();
        var algo = new ScaleProperties(graph, config, AllocationTracker.empty());
        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1 / 13D, -0.5D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8 / 13D, -0.25D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D, 0}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9 / 13D, 0.25D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D, 0.5D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }
}
