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
package org.neo4j.graphalgo.beta.pregel;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.DOUBLE_ARRAY_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.DOUBLE_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.LONG_ARRAY_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.LONG_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.TestPregelComputation.KEY;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@GdlExtension
class PregelFJTest {

    @GdlGraph
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (alice:Node { doubleSeed: 42.0, longSeed: 23 })" +
        ", (bob:Node { doubleSeed: 43.0, longSeed: 24 })" +
        ", (eve:Node { doubleSeed: 44.0, longSeed: 25 })" +
        ", (alice)-[:REL {prop: 2.0}]->(bob)" +
        ", (alice)-[:REL {prop: 1.0}]->(eve)";

    @Inject
    private TestGraph graph;

    @ParameterizedTest
    @MethodSource("configAndResult")
    <C extends PregelConfig> void sendsMessages(C config, PregelComputation<C> computation, double[] expected) {
        PregelFJ<C> pregelJob = PregelFJ.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertArrayEquals(expected, nodeValues.doubleProperties(KEY).toArray());
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void testCorrectnessForLargeGraph(Partitioning partitioningScheme) {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(10_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(42L)
            .allocationTracker(AllocationTracker.empty())
            .build()
            .generate();

        var configBuilder = ImmutablePregelConfig.builder()
            .username("")
            .maxIterations(10)
            .partitioning(partitioningScheme)
            .isAsynchronous(false);

        var singleThreadedConfig = configBuilder.concurrency(1).build();
        var multiThreadedConfig = configBuilder.concurrency(4).build();

        var singleThreaded = run(graph, singleThreadedConfig, new PregelTest.TestPregelComputation());
        var singleThreadedReduce = run(graph, singleThreadedConfig, new PregelTest.TestReduciblePregelComputation());

        var multiThreaded = run(graph, multiThreadedConfig, new PregelTest.TestPregelComputation());
        var multiThreadedReduce = run(graph, multiThreadedConfig, new PregelTest.TestReduciblePregelComputation());

        for (int nodeId = 0; nodeId < singleThreaded.size(); nodeId++) {
            var v1 = singleThreaded.get(nodeId);
            var v2 = singleThreadedReduce.get(nodeId);
            var v3 = multiThreaded.get(nodeId);
            var v4 = multiThreadedReduce.get(nodeId);
            assertTrue(
                v1 == v2 && v1 == v3 && v1 == v4,
                formatWithLocale("Value mismatch for node id %d: %f, %f, %f, %f", nodeId, v1, v2, v3, v4)
            );
        }
    }

    @NotNull
    private HugeDoubleArray run(Graph graph, PregelConfig config, PregelComputation<PregelConfig> computation) {
        var pregelJob = PregelFJ.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        return pregelJob.run().nodeValues().doubleProperties(KEY);
    }

    @Test
    void sendMessageToSpecificTarget() {
        var config = ImmutablePregelConfig.builder()
            .maxIterations(2)
            .concurrency(1)
            .build();

        var pregelJob = PregelFJ.create(
            graph,
            config,
            new PregelTest.TestSendTo(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertEquals(2.0, nodeValues.doubleProperties(KEY).get(0L));
        assertEquals(Double.NaN, nodeValues.doubleProperties(KEY).get(1L));
        assertEquals(Double.NaN, nodeValues.doubleProperties(KEY).get(2L));
    }

    @Test
    void compositeNodeValueTest() {
        var config = ImmutableCompositeTestComputationConfig.builder()
            .maxIterations(2)
            .concurrency(1)
            .longProperty("longSeed")
            .doubleProperty("doubleSeed")
            .build();

        var pregelJob = PregelFJ.create(
            graph,
            config,
            new PregelTest.CompositeTestComputation(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        var result = pregelJob.run().nodeValues();

        assertEquals(46L, result.longValue(LONG_KEY, graph.toOriginalNodeId("alice")));
        assertEquals(84.0D, result.doubleValue(DOUBLE_KEY, graph.toOriginalNodeId("alice")));
        assertArrayEquals(new long[]{46L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toOriginalNodeId("alice")));
        assertArrayEquals(
            new double[]{84.0D},
            result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toOriginalNodeId("alice"))
        );

        assertEquals(48L, result.longValue(LONG_KEY, graph.toOriginalNodeId("bob")));
        assertEquals(86.0D, result.doubleValue(DOUBLE_KEY, graph.toOriginalNodeId("bob")));
        assertArrayEquals(new long[]{48L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toOriginalNodeId("bob")));
        assertArrayEquals(
            new double[]{86.0D},
            result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toOriginalNodeId("bob"))
        );

        assertEquals(50L, result.longValue(LONG_KEY, graph.toOriginalNodeId("eve")));
        assertEquals(88.0D, result.doubleValue(DOUBLE_KEY, graph.toOriginalNodeId("eve")));
        assertArrayEquals(new long[]{50L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toOriginalNodeId("eve")));
        assertArrayEquals(
            new double[]{88.0D},
            result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toOriginalNodeId("eve"))
        );
    }

    @Test
    void testMasterComputeStep() {
        var pregelJob = PregelFJ.create(
            graph,
            ImmutablePregelConfig.builder().maxIterations(4).build(),
            new PregelTest.TestMasterCompute(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertArrayEquals(new long[]{4L, 4L, 4L}, nodeValues.longProperties(KEY).toArray());
    }

    static Stream<Arguments> estimations() {
        return Stream.of(
            // queue based sync
            Arguments.of(1, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, false, 7441696L),
            Arguments.of(10, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, false, 7442344L),
            Arguments.of(1, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                false,
                9441768L
            ),
            Arguments.of(10, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                false,
                9442416L
            ),

            // queue based async
            Arguments.of(1, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, true, 3841656L),
            Arguments.of(10, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, true, 3842304L),
            Arguments.of(1, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                true,
                5841728L
            ),
            Arguments.of(10, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                true,
                5842376L
            ),

            // array based
            Arguments.of(1, new PregelSchema.Builder().add("key", ValueType.LONG).build(), false, false, 241_576L),
            Arguments.of(10, new PregelSchema.Builder().add("key", ValueType.LONG).build(), false, false, 242_224L),
            Arguments.of(1, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                false,
                false,
                2_241_648L
            ),
            Arguments.of(10, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                false,
                false,
                2_242_296L
            )
        );
    }

    @ParameterizedTest
    @MethodSource("estimations")
    void memoryEstimation(int concurrency, PregelSchema pregelSchema, boolean isQueueBased, boolean isAsync, long expectedBytes) {
        var dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(10_000)
            .maxRelCount(100_000)
            .build();

        assertEquals(
            MemoryRange.of(expectedBytes).max,
            Pregel.memoryEstimation(pregelSchema, isQueueBased, isAsync).estimate(dimensions, concurrency).memoryUsage().max
        );
    }

    static Stream<Arguments> configAndResult() {
        return Stream.of(
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).build(),
                new PregelTest.TestPregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).relationshipWeightProperty("prop").build(),
                new PregelTest.TestPregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).relationshipWeightProperty("prop").build(),
                new PregelTest.TestWeightComputation(),
                new double[]{0.0, 2.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).build(),
                new PregelTest.TestReduciblePregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            )
        );
    }

    @Test
    void preventIllegalConcurrencyConfiguration() {
        var config = ImmutableHackerManConfig.builder()
            .maxIterations(1337)
            .concurrency(42)
            .build();

        assertThrows(IllegalArgumentException.class, () -> PregelFJ.create(
            graph,
            config,
            new PregelTest.TestSendTo(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        ));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void messagesInInitialSuperStepShouldBeEmpty(boolean isAsynchronous) {
        var pregelJob = PregelFJ.create(
            graph,
            ImmutablePregelConfig.builder().maxIterations(2).isAsynchronous(isAsynchronous).build(),
            new PregelTest.TestEmptyMessageInInitialSuperstep(),
            Pools.DEFAULT,
            AllocationTracker.empty()
        );

        // assertion is happening in the computation
        pregelJob.run();
    }
}
