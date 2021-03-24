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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@GdlExtension
class ScalePropertiesTest {

    @GdlGraph
    static String GDL =
        "(a:A {a: 1.1D, b: 20, c: 50, array:[1.0,2.0]}), " +
        "(b:A {a: 2.8D, b: 21, c: 51}), " +
        "(c:A {a: 3, b: 22, c: 52}), " +
        "(d:A {a: -1, b: 23, c: 60}), " +
        "(e:A {a: -10, b: 24, c: 100})";

    @Inject
    TestGraph graph;

    @Test
    void minmaxScaling() {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("a"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .concurrency(1)
            .build();
        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1 / 13D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8 / 13D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9 / 13D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    private static Stream<Arguments> scalers() {
        return Stream.of(
            Arguments.of(List.of(Scaler.Variant.MINMAX)),
            Arguments.of(List.of(Scaler.Variant.MINMAX, Scaler.Variant.MINMAX, Scaler.Variant.MINMAX))
        );
    }

    @ParameterizedTest
    @MethodSource("scalers")
    void minmaxScalingOverMultipleProperties(List<Scaler.Variant> scalers) {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("a", "b", "c"))
            .scalers(scalers)
            .concurrency(1)
            .build();
        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1 / 13D, 0D, 0D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8 / 13D, 0.25, 1 / 50D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D, 0.5, 2 / 50D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9 / 13D, 0.75, 10 / 50D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D, 1D, 1D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    @Test
    void differentScalers() {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("a", "b", "c"))
            .scalers(List.of(Scaler.Variant.MINMAX, Scaler.Variant.MEAN, Scaler.Variant.LOG))
            .concurrency(1)
            .build();
        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);
        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1 / 13D, -0.5D, 3.912023005428146D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8 / 13D, -0.25D, 3.9318256327243257D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D, 0, 3.9512437185814275D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9 / 13D, 0.25D, 4.0943445622221D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D, 0.5D, 4.605170185988092D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    @Test
    void parallelScaler() {
        int nodeCount = 50_000;
        var bigGraph = RandomGraphGenerator
            .builder()
            .nodeCount(nodeCount)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(PropertyProducer.random("a", -100, 100))
            .build()
            .generate();

        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("a"))
            .scalers(List.of(Scaler.Variant.MINMAX));

        var parallelResult = new ScaleProperties(
            bigGraph,
            config.concurrency(4).build(),
            AllocationTracker.empty(),
            Pools.DEFAULT
        ).compute().scaledProperties();

        var expected = new ScaleProperties(
            bigGraph,
            config.concurrency(1).build(),
            AllocationTracker.empty(),
            Pools.DEFAULT
        ).compute().scaledProperties();

        IntStream.range(0, nodeCount).forEach(id -> assertEquals(expected.get(id)[0], parallelResult.get(id)[0]));
    }

    @Test
    void failOnArrayProperty() {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("array"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .build();

        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);
        var error = assertThrows(UnsupportedOperationException.class, algo::compute);

        assertThat(error.getMessage(), containsString("Scaling node property `array` of type `List of Float` is not supported"));
    }

    @Test
    void failOnNonExistentProperty() {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("IMAGINARY_PROP"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .build();

        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);
        var error = assertThrows(IllegalArgumentException.class, algo::compute);

        assertThat(error.getMessage(), containsString("Node property `IMAGINARY_PROP` not found in graph"));
    }
}
