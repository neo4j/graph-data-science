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
package org.neo4j.gds.scaleproperties;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.scaling.MinMax;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class ScalePropertiesTest {

    @GdlGraph
    static String GDL =
        "(a:A {a: 1.1D, b: 20, c: 50, bAndC: [20.0, 50.0], longArrayB: [20L], floatArrayB: [20.0], doubleArray: [1.000000001d],  mixedSizeArray: [1.0, 1.0], missingArray: [1.0,2.0]}), " +
        "(b:A {a: 2.8D, b: 21, c: 51, bAndC: [21.0, 51.0], longArrayB: [21L], floatArrayB: [21.0], doubleArray: [1.000000002d], mixedSizeArray: [1.0]}), " +
        "(c:A {a: 3, b: 22, c: 52, bAndC: [22.0, 52.0], longArrayB: [22L], floatArrayB: [22.0], doubleArray: [1.000000003d], mixedSizeArray: [1.0]}), " +
        "(d:A {a: -1, b: 23, c: 60, bAndC: [23.0, 60.0], longArrayB: [23L], floatArrayB: [23.0], doubleArray: [1.000000004d], mixedSizeArray: [1.0]}), " +
        "(e:A {a: -10, b: 24, c: 100, bAndC: [24.0, 100.0], longArrayB: [24L], floatArrayB: [24.0], doubleArray: [1.000000005d], mixedSizeArray: [1.0, 2.0, 3.0]})";

    @Inject
    TestGraph graph;

    @Test
    void scaleSingleProperty() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("a"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .concurrency(1)
            .build();
        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, Pools.DEFAULT);

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1 / 13D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8 / 13D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9 / 13D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    @Test
    void scaleMultipleProperties() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("a", "b", "c"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .concurrency(1)
            .build();
        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, Pools.DEFAULT);

        var result = algo.compute();
        var resultProperties = result.scaledProperties().toArray();

        assertArrayEquals(new double[]{11.1 / 13D, 0D, 0D}, resultProperties[(int) graph.toOriginalNodeId("a")]);
        assertArrayEquals(new double[]{12.8 / 13D, 0.25, 1 / 50D}, resultProperties[(int) graph.toOriginalNodeId("b")]);
        assertArrayEquals(new double[]{1D, 0.5, 2 / 50D}, resultProperties[(int) graph.toOriginalNodeId("c")]);
        assertArrayEquals(new double[]{9 / 13D, 0.75, 10 / 50D}, resultProperties[(int) graph.toOriginalNodeId("d")]);
        assertArrayEquals(new double[]{0D, 1D, 1D}, resultProperties[(int) graph.toOriginalNodeId("e")]);
    }

    @Test
    void parallelScale() {
        int nodeCount = 50_000;
        var bigGraph = RandomGraphGenerator
            .builder()
            .nodeCount(nodeCount)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(PropertyProducer.randomDouble("a", -100, 100))
            .build()
            .generate();

        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("a"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()));

        var parallelResult = new ScaleProperties(
            bigGraph,
            config.concurrency(4).build(),
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute().scaledProperties();

        var expected = new ScaleProperties(
            bigGraph,
            config.concurrency(1).build(),
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute().scaledProperties();

        IntStream.range(0, nodeCount).forEach(id -> assertEquals(expected.get(id)[0], parallelResult.get(id)[0]));
    }

    @Test
    void scaleArrayProperty() {
        var arrayConfig = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("a", "bAndC", "longArrayB"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .build();

        var actual = new ScaleProperties(graph, arrayConfig, ProgressTracker.NULL_TRACKER, Pools.DEFAULT)
            .compute()
            .scaledProperties();

        var singlePropConfig = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("a", "b", "c", "longArrayB"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .build();

        var expected = new ScaleProperties(graph, singlePropConfig, ProgressTracker.NULL_TRACKER, Pools.DEFAULT)
            .compute()
            .scaledProperties();

        LongStream.range(0, graph.nodeCount()).forEach(id -> assertArrayEquals(expected.get(id), actual.get(id)));
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfigTest#scalers")
    void supportLongAndFloatArrays(String scaler) {
        var baseConfigBuilder = ScalePropertiesStreamConfigImpl.builder()
            .scaler(ScalerFactory.SUPPORTED_SCALERS.get(scaler).apply(CypherMapWrapper.empty()));
        var bConfig = baseConfigBuilder.nodeProperties(List.of("b")).build();
        var longArrayBConfig = baseConfigBuilder.nodeProperties(List.of("longArrayB")).build();
        var doubleArrayBConfig = baseConfigBuilder.nodeProperties(List.of("floatArrayB")).build();

        var expected = new ScaleProperties(graph, bConfig, ProgressTracker.NULL_TRACKER, Pools.DEFAULT)
            .compute()
            .scaledProperties();
        var actualLong = new ScaleProperties(graph, longArrayBConfig, ProgressTracker.NULL_TRACKER, Pools.DEFAULT)
            .compute()
            .scaledProperties();
        var actualDouble = new ScaleProperties(graph, doubleArrayBConfig, ProgressTracker.NULL_TRACKER, Pools.DEFAULT)
            .compute()
            .scaledProperties();

        LongStream.range(0, graph.nodeCount()).forEach(id -> assertArrayEquals(expected.get(id), actualLong.get(id)));
        LongStream.range(0, graph.nodeCount()).forEach(id -> assertArrayEquals(expected.get(id), actualDouble.get(id)));
    }

    @Test
    void supportDoubleArrays() {
        var baseConfigBuilder = ScalePropertiesStreamConfigImpl
            .builder()
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()));
        var config = baseConfigBuilder.nodeProperties(List.of("doubleArray")).build();

        var expected = new double[][]{new double[]{0.0}, new double[]{0.2499999722444236}, new double[]{.5}, new double[]{0.7500000277555764}, new double[]{1.0}};
        var actual = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, Pools.DEFAULT)
            .compute()
            .scaledProperties();

        IntStream.range(0, (int) graph.nodeCount()).forEach(id -> assertArrayEquals(expected[id], actual.get(id)));
    }

    @Test
    void failOnArrayPropertyWithUnequalLength() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("mixedSizeArray"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .build();

        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, Pools.DEFAULT);
        var error = assertThrows(IllegalArgumentException.class, algo::compute);

        assertThat(error.getMessage(), containsString(
            "For scaling property `mixedSizeArray` expected array of length 2 but got length 1 for node 1"
        ));
    }

    @Test
    void failOnMissingValuesForArrayProperty() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("missingArray"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .build();

        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, Pools.DEFAULT);
        var error = assertThrows(IllegalArgumentException.class, algo::compute);

        assertThat(error.getMessage(), containsString(
            "For scaling property `missingArray` expected array of length 2 but got length 0 for node 1"
        ));
    }

    @Test
    void failOnNonExistentProperty() {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("IMAGINARY_PROP"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .build();

        var algo = new ScaleProperties(graph, config, ProgressTracker.NULL_TRACKER, Pools.DEFAULT);
        var error = assertThrows(IllegalArgumentException.class, algo::compute);

        assertThat(error.getMessage(), containsString("Node property `IMAGINARY_PROP` not found in graph"));
    }

    @Test
    void progressLogging() {
        var graph = RandomGraphGenerator
            .builder()
            .nodeCount(1_000)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(PropertyProducer.randomLong("data1", -9, 420))
            .nodePropertyProducer(PropertyProducer.randomEmbedding("data2", 64, -1337, 1337))
            .nodePropertyProducer(PropertyProducer.randomLong("data3", -9, 420))
            .build()
            .generate();

        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("data1", "data2", "data3"))
            .scaler(MinMax.buildFrom(CypherMapWrapper.empty()))
            .build();

        var factory = new ScalePropertiesFactory<>();
        var testLog = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            factory.progressTask(graph, config),
            testLog,
            1,
            EmptyTaskRegistryFactory.INSTANCE
        );

        factory.build(graph, config, progressTracker).compute();

        assertEquals(3, progressTracker.getProgresses().size());
        Assertions.assertThat(testLog.getMessages(INFO))
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .hasSize(133)
            .containsSequence(
                "ScaleProperties :: Start",
                "ScaleProperties :: Prepare scalers :: Start",
                "ScaleProperties :: Prepare scalers 7%",
                "ScaleProperties :: Prepare scalers 9%",
                "ScaleProperties :: Prepare scalers 10%"
            ).containsSequence(
                "ScaleProperties :: Prepare scalers 96%",
                "ScaleProperties :: Prepare scalers 98%",
                "ScaleProperties :: Prepare scalers 100%",
                "ScaleProperties :: Prepare scalers :: Finished",
                "ScaleProperties :: Scale properties :: Start",
                "ScaleProperties :: Scale properties 1%",
                "ScaleProperties :: Scale properties 2%",
                "ScaleProperties :: Scale properties 3%"
            ).containsSequence(
                "ScaleProperties :: Scale properties 96%",
                "ScaleProperties :: Scale properties 98%",
                "ScaleProperties :: Scale properties 100%",
                "ScaleProperties :: Scale properties :: Finished",
                "ScaleProperties :: Finished"
            );
    }

    @ParameterizedTest
    @CsvSource(value = {
        // BASE
        "   1_000, 1_044_064",

        // Should increase linearly with node count
        "   2_000, 2_088_064"
    })
    void shouldEstimateMemory(
        long nodeCount,
        long expectedMemory
    ) {
        var config = ScalePropertiesStreamConfigImpl.builder()
            .nodeProperties(List.of("DUMMY"))
            .scaler("MEAN")
            .build();

        assertMemoryEstimation(
            () -> new ScalePropertiesFactory<>().memoryEstimation(config),
            nodeCount,
            1,
            MemoryRange.of(expectedMemory)
        );
    }
}
