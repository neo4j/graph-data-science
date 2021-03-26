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
import org.junit.jupiter.params.provider.EnumSource;
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
import java.util.stream.LongStream;
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
        "(a:A {a: 1.1D, b: 20, c: 50, bAndC: [20.0, 50.0], longArrayB: [20L], doubleArrayB: [20.0d], mixedSizeArray: [1.0, 1.0], missingArray: [1.0,2.0]}), " +
        "(b:A {a: 2.8D, b: 21, c: 51, bAndC: [21.0, 51.0], longArrayB: [21L], doubleArrayB: [21.0d], mixedSizeArray: [1.0]}), " +
        "(c:A {a: 3, b: 22, c: 52, bAndC: [22.0, 52.0], longArrayB: [22L], doubleArrayB: [22.0d], mixedSizeArray: [1.0]}), " +
        "(d:A {a: -1, b: 23, c: 60, bAndC: [23.0, 60.0], longArrayB: [23L], doubleArrayB: [23.0d], mixedSizeArray: [1.0]}), " +
        "(e:A {a: -10, b: 24, c: 100, bAndC: [24.0, 100.0], longArrayB: [24L], doubleArrayB: [24.0d], mixedSizeArray: [1.0, 2.0, 3.0]})";

    @Inject
    TestGraph graph;

    @Test
    void scaleSingleProperty() {
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
    void scaleMultipleProperties(List<Scaler.Variant> scalers) {
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
    void scaleWithDifferentScalers() {
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
    void parallelScale() {
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
    void scaleArrayProperty() {
        var arrayConfig = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("a", "bAndC", "a"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .build();

        var actual = new ScaleProperties(graph, arrayConfig, AllocationTracker.empty(), Pools.DEFAULT)
            .compute()
            .scaledProperties();

        var singlePropConfig = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("a", "b", "c", "a"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .build();

        var expected = new ScaleProperties(graph, singlePropConfig, AllocationTracker.empty(), Pools.DEFAULT)
            .compute()
            .scaledProperties();

        LongStream.range(0, graph.nodeCount()).forEach(id -> assertArrayEquals(expected.get(id), actual.get(id)));
    }

    @ParameterizedTest
    @EnumSource(Scaler.Variant.class)
    void supportLongAndDoubleArrays(Scaler.Variant scaler) {
        var baseConfigBuilder = ImmutableScalePropertiesBaseConfig.builder()
            .scalers(List.of(scaler));
        var bConfig = baseConfigBuilder.nodeProperties(List.of("b")).build();
        var longArrayBConfig = baseConfigBuilder.nodeProperties(List.of("longArrayB")).build();
        var doubleArrayBConfig = baseConfigBuilder.nodeProperties(List.of("doubleArrayB")).build();

        var expected = new ScaleProperties(graph, bConfig, AllocationTracker.empty(), Pools.DEFAULT).compute().scaledProperties();
        var actualLong = new ScaleProperties(graph, longArrayBConfig, AllocationTracker.empty(), Pools.DEFAULT).compute().scaledProperties();
        var actualDouble = new ScaleProperties(graph, doubleArrayBConfig, AllocationTracker.empty(), Pools.DEFAULT).compute().scaledProperties();

        LongStream.range(0, graph.nodeCount()).forEach(id -> assertArrayEquals(expected.get(id), actualLong.get(id)));
        LongStream.range(0, graph.nodeCount()).forEach(id -> assertArrayEquals(expected.get(id), actualDouble.get(id)));
    }

    @Test
    void failOnArrayPropertyWithUnequalLength() {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("mixedSizeArray"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .build();

        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);
        var error = assertThrows(IllegalArgumentException.class, algo::compute);

        assertThat(error.getMessage(), containsString(
            "For scaling property `mixedSizeArray` expected array of length 2 but got length 1 for node 1"
        ));
    }

    @Test
    void failOnMissingValuesForArrayProperty() {
        var config = ImmutableScalePropertiesBaseConfig.builder()
            .nodeProperties(List.of("missingArray"))
            .scalers(List.of(Scaler.Variant.MINMAX))
            .build();

        var algo = new ScaleProperties(graph, config, AllocationTracker.empty(), Pools.DEFAULT);
        var error = assertThrows(IllegalArgumentException.class, algo::compute);

        assertThat(error.getMessage(), containsString(
            "For scaling property `missingArray` expected array of length 2 but got length 0 for node 1"
        ));
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
