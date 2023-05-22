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
package org.neo4j.gds.beta.pregel;

import org.assertj.core.api.ThrowableAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.GdlBuilder;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.TestTaskStore;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.ComputeContext.BidirectionalComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.beta.pregel.context.MasterComputeContext;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.assertTransactionTermination;
import static org.neo4j.gds.TestSupport.crossArguments;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.beta.pregel.PregelTest.CompositeTestComputation.DOUBLE_ARRAY_KEY;
import static org.neo4j.gds.beta.pregel.PregelTest.CompositeTestComputation.DOUBLE_KEY;
import static org.neo4j.gds.beta.pregel.PregelTest.CompositeTestComputation.LONG_ARRAY_KEY;
import static org.neo4j.gds.beta.pregel.PregelTest.CompositeTestComputation.LONG_KEY;
import static org.neo4j.gds.beta.pregel.PregelTest.TestPregelComputation.KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class PregelTest {

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
    @MethodSource("partitioningConfigAndResult")
    void sendsMessages(
        Partitioning partitioning,
        ImmutablePregelConfig.Builder configBuilder,
        PregelComputation<PregelConfig> computation,
        double[] expected
    ) {
        var config = configBuilder.partitioning(partitioning).build();

        Pregel<PregelConfig> pregelJob = Pregel.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertArrayEquals(expected, nodeValues.doubleProperties(KEY).toArray());
    }

    @Test
    void stopsEarlyWhenTransactionHasBeenTerminated() {
        TerminationFlag terminationFlag = () -> false;

        var config = PregelConfigImpl.builder().maxIterations(10).build();

        Pregel<PregelConfig> pregelJob = Pregel.create(
            graph,
            config,
            new TestPregelComputation(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );
        pregelJob.setTerminationFlag(terminationFlag);

        assertTransactionTermination(pregelJob::run);
    }

    @ParameterizedTest
    @ValueSource(strings = {"AUTO", "RANGE"})
    void logProgress(Partitioning partitioning) {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(40_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(42L)
            .build()
            .generate();

        var config = ImmutablePregelConfig.builder()
            .maxIterations(2)
            .partitioning(partitioning)
            .concurrency(4)
            .isAsynchronous(false)
            .build();

        var computation = new TestPregelComputation();

        var task = Pregel.progressTask(graph, config, computation.getClass().getSimpleName());
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(task, log, config.concurrency(), EmptyTaskRegistryFactory.INSTANCE);

        Pregel.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            progressTracker
        ).run();

        assertThat(progressTracker.getProgresses())
            .extracting(AtomicLong::get)
            .allMatch(progress -> progress == 0 || progress == graph.nodeCount());

        assertThat(log.getMessages(TestLog.INFO))
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .contains(
                "TestPregelComputation :: Compute iteration 1 of 2 :: Start",
                "TestPregelComputation :: Compute iteration 1 of 2 25%",
                "TestPregelComputation :: Compute iteration 1 of 2 50%",
                "TestPregelComputation :: Compute iteration 1 of 2 75%",
                "TestPregelComputation :: Compute iteration 1 of 2 100%",
                "TestPregelComputation :: Compute iteration 1 of 2 :: Finished",
                "TestPregelComputation :: Master compute iteration 1 of 2 :: Start",
                "TestPregelComputation :: Master compute iteration 1 of 2 :: Finished",

                "TestPregelComputation :: Compute iteration 2 of 2 :: Start",
                "TestPregelComputation :: Compute iteration 2 of 2 25%",
                "TestPregelComputation :: Compute iteration 2 of 2 50%",
                "TestPregelComputation :: Compute iteration 2 of 2 75%",
                "TestPregelComputation :: Compute iteration 2 of 2 100%",
                "TestPregelComputation :: Compute iteration 2 of 2 :: Finished",
                "TestPregelComputation :: Master compute iteration 2 of 2 :: Start",
                "TestPregelComputation :: Master compute iteration 2 of 2 :: Finished"
            );
    }

    @Test
    void cleanupProgressLogging() {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(42)
            .averageDegree(2)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(1337L)
            .build()
            .generate();

        var config = ImmutablePregelConfig.builder()
            .maxIterations(2)
            .isAsynchronous(false)
            .build();

        var taskStore = new TestTaskStore();
        var computation = new TestPregelComputation();

        var task = Pregel.progressTask(graph, config, computation.getClass().getSimpleName());
        var progressTracker = new TaskProgressTracker(
            task,
            Neo4jProxy.testLog(),
            config.concurrency(),
            jobId -> new TaskRegistry("", taskStore, jobId)
        );

        var pregelAlgo = Pregel.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            progressTracker
        );

        pregelAlgo.run();
        pregelAlgo.release();

        assertThat(taskStore.tasks()).isEmpty();
        assertThat(taskStore.tasksSeen())
            .containsExactlyInAnyOrder("TestPregelComputation");
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void testCorrectnessForLargeGraph(Partitioning partitioning) {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(10_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(42L)
            .build()
            .generate();

        var configBuilder = ImmutablePregelConfig.builder()
            .maxIterations(10)
            .partitioning(partitioning)
            .isAsynchronous(false);

        var singleThreadedConfig = configBuilder.concurrency(1).build();
        var multiThreadedConfig = configBuilder.concurrency(4).build();

        var singleThreaded = run(graph, singleThreadedConfig, new TestPregelComputation());
        var singleThreadedReduce = run(graph, singleThreadedConfig, new TestReduciblePregelComputation());

        var multiThreaded = run(graph, multiThreadedConfig, new TestPregelComputation());
        var multiThreadedReduce = run(graph, multiThreadedConfig, new TestReduciblePregelComputation());

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
        var pregelJob = Pregel.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        return pregelJob.run().nodeValues().doubleProperties(KEY);
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void sendMessageToSpecificTarget(Partitioning partitioning) {
        var config = ImmutablePregelConfig.builder()
            .maxIterations(2)
            .concurrency(1)
            .partitioning(partitioning)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new TestSendTo(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertEquals(2.0, nodeValues.doubleProperties(KEY).get(0L));
        assertEquals(Double.NaN, nodeValues.doubleProperties(KEY).get(1L));
        assertEquals(Double.NaN, nodeValues.doubleProperties(KEY).get(2L));
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void compositeNodeValueTest(Partitioning partitioning) {
        var config = ImmutableCompositeTestComputationConfig.builder()
            .maxIterations(2)
            .concurrency(1)
            .partitioning(partitioning)
            .longProperty("longSeed")
            .doubleProperty("doubleSeed")
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new CompositeTestComputation(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run().nodeValues();

        assertEquals(46L, result.longValue(LONG_KEY, graph.toMappedNodeId("alice")));
        assertEquals(84.0D, result.doubleValue(DOUBLE_KEY, graph.toMappedNodeId("alice")));
        assertArrayEquals(new long[]{46L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toMappedNodeId("alice")));
        assertArrayEquals(
            new double[]{84.0D},
            result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toMappedNodeId("alice"))
        );

        assertEquals(48L, result.longValue(LONG_KEY, graph.toMappedNodeId("bob")));
        assertEquals(86.0D, result.doubleValue(DOUBLE_KEY, graph.toMappedNodeId("bob")));
        assertArrayEquals(new long[]{48L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toMappedNodeId("bob")));
        assertArrayEquals(
            new double[]{86.0D},
            result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toMappedNodeId("bob"))
        );

        assertEquals(50L, result.longValue(LONG_KEY, graph.toMappedNodeId("eve")));
        assertEquals(88.0D, result.doubleValue(DOUBLE_KEY, graph.toMappedNodeId("eve")));
        assertArrayEquals(new long[]{50L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toMappedNodeId("eve")));
        assertArrayEquals(
            new double[]{88.0D},
            result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toMappedNodeId("eve"))
        );
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void testMasterComputeStep(Partitioning partitioning) {
        var pregelJob = Pregel.create(
            graph,
            ImmutablePregelConfig.builder().maxIterations(4).partitioning(partitioning).build(),
            new TestMasterCompute(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertArrayEquals(new long[]{4L, 4L, 4L}, nodeValues.longProperties(KEY).toArray());
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void testMasterComputeStepWithConvergence(Partitioning partitioning) {
        var pregelJob = Pregel.create(
            graph,
            ImmutablePregelConfig.builder().maxIterations(4).partitioning(partitioning).build(),
            new TestMasterCompute(2),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        var result = pregelJob.run();
        assertThat(result.didConverge()).isTrue();
        assertThat(result.ranIterations()).isEqualTo(2);

        var nodeValues = result.nodeValues();
        assertArrayEquals(new long[]{3L, 3L, 3L}, nodeValues.longProperties(KEY).toArray());
    }

    static Stream<Arguments> estimations() {
        return Stream.of(
            // queue based sync
            Arguments.of(1, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, false, 7441704L),
            Arguments.of(10, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, false, 7442208L),
            Arguments.of(1, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                false,
                9441776L
            ),
            Arguments.of(10, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                false,
                9442280L
            ),

            // queue based async
            Arguments.of(1, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, true, 3841664L),
            Arguments.of(10, new PregelSchema.Builder().add("key", ValueType.LONG).build(), true, true, 3842168L),
            Arguments.of(1, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                true,
                5841736L
            ),
            Arguments.of(10, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                true,
                true,
                5842240L
            ),

            // array based
            Arguments.of(1, new PregelSchema.Builder().add("key", ValueType.LONG).build(), false, false, 241584L),
            Arguments.of(10, new PregelSchema.Builder().add("key", ValueType.LONG).build(), false, false, 242088L),
            Arguments.of(1, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                false,
                false,
                2241656L
            ),
            Arguments.of(10, new PregelSchema.Builder()
                    .add("key1", ValueType.LONG)
                    .add("key2", ValueType.DOUBLE)
                    .add("key3", ValueType.LONG_ARRAY)
                    .add("key4", ValueType.DOUBLE_ARRAY)
                    .build(),
                false,
                false,
                2242160L
            )
        );
    }

    @Test
    void testIdMapping() {
        var idSupplier = new AtomicLong(42);
        var graph = new GdlBuilder()
            .gdl("(a)")
            .idSupplier(idSupplier::getAndIncrement)
            .build();

        var originalId = graph.toOriginalNodeId("a");
        assertThat(originalId).isEqualTo(42);

        var ranInit = new AtomicBoolean(false);
        var ranCompute = new AtomicBoolean(false);
        var ranMaster = new AtomicBoolean(false);

        Pregel.create(
            graph,
            ImmutablePregelConfig.builder().maxIterations(1).build(),
            new PregelComputation<>() {

                @Override
                public PregelSchema schema(PregelConfig config) {
                    return new PregelSchema.Builder().add("foo", ValueType.LONG).build();
                }

                @Override
                public void init(InitContext<PregelConfig> context) {
                    assertThat(context.toOriginalId()).isEqualTo(originalId);
                    assertThat(context.toOriginalId(context.nodeId())).isEqualTo(originalId);
                    assertThat(context.toInternalId(originalId)).isEqualTo(context.nodeId());
                    ranInit.set(true);
                }

                @Override
                public void compute(ComputeContext<PregelConfig> context, Messages messages) {
                    assertThat(context.toOriginalId()).isEqualTo(originalId);
                    assertThat(context.toOriginalId(context.nodeId())).isEqualTo(originalId);
                    assertThat(context.toInternalId(originalId)).isEqualTo(context.nodeId());
                    ranCompute.set(true);
                }

                @Override
                public boolean masterCompute(MasterComputeContext<PregelConfig> context) {
                    assertThat(context.toOriginalId(0)).isEqualTo(originalId);
                    assertThat(context.toInternalId(originalId)).isEqualTo(0);
                    ranMaster.set(true);
                    return true;
                }
            },
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ).run();

        assertThat(ranInit.get()).isTrue();
        assertThat(ranCompute.get()).isTrue();
        assertThat(ranMaster.get()).isTrue();
    }

    @ParameterizedTest
    @MethodSource("estimations")
    void memoryEstimation(
        int concurrency,
        PregelSchema pregelSchema,
        boolean isQueueBased,
        boolean isAsync,
        long expectedBytes
    ) {
        var dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(10_000)
            .relCountUpperBound(100_000)
            .build();

        assertEquals(
            MemoryRange.of(expectedBytes).max,
            Pregel
                .memoryEstimation(pregelSchema, isQueueBased, isAsync)
                .estimate(dimensions, concurrency)
                .memoryUsage().max
        );
    }

    static Stream<Arguments> partitioningConfigAndResult() {
        return crossArguments(PregelTest::partitionings, PregelTest::configAndResult);
    }

    static Stream<Arguments> configAndResult() {
        return Stream.of(
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2),
                new TestPregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).relationshipWeightProperty("prop"),
                new TestPregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).relationshipWeightProperty("prop"),
                new TestWeightComputation(),
                new double[]{0.0, 2.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2),
                new TestReduciblePregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            )
        );
    }

    @ValueClass
    @SuppressWarnings("immutables:subtype")
    interface HackerManConfig extends PregelProcedureConfig {
        @Override
        default void validateConcurrency() {
            // haha, h4ck3rm4n, so smart, much wow
        }

        @Override
        default void validateWriteConcurrency() {
            // and he strikes again, HAHA
        }
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void preventIllegalConcurrencyConfiguration(Partitioning partitioning) {
        var config = ImmutableHackerManConfig.builder()
            .maxIterations(1337)
            .partitioning(partitioning)
            .concurrency(42)
            .build();

        assertThrows(IllegalArgumentException.class, () -> Pregel.create(
            graph,
            config,
            new TestSendTo(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        ));
    }

    static Stream<Arguments> partitioningAndAsynchronous() {
        return crossArguments(PregelTest::partitionings, TestSupport::trueFalseArguments);
    }

    @ParameterizedTest
    @MethodSource("partitioningAndAsynchronous")
    void messagesInInitialSuperStepShouldBeEmpty(Partitioning partitioning, boolean isAsynchronous) {
        var config = ImmutablePregelConfig
            .builder()
            .maxIterations(2)
            .partitioning(partitioning)
            .isAsynchronous(isAsynchronous)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new TestEmptyMessageInInitialSuperstep(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        // assertion is happening in the computation
        pregelJob.run();
    }

    static Stream<Arguments> partitionings() {
        return Arrays.stream(Partitioning.values()).map(Arguments::of);
    }

    public static class TestPregelComputation implements PregelComputation<PregelConfig> {

        static final String KEY = "value";

        @Override
        public PregelSchema schema(PregelConfig config) {
            return new PregelSchema.Builder()
                .add(KEY, ValueType.DOUBLE)
                .build();
        }

        @Override
        public void compute(ComputeContext<PregelConfig> context, Messages messages) {
            if (context.isInitialSuperstep()) {
                context.setNodeValue(KEY, 0.0);
                context.sendToNeighbors(1.0);
            } else {
                double messageSum = 0.0;
                for (Double message : messages) {
                    messageSum += message.longValue();
                }
                context.setNodeValue(KEY, messageSum);
            }
            context.voteToHalt();
        }
    }

    public static class TestReduciblePregelComputation extends TestPregelComputation {

        @Override
        public Optional<Reducer> reducer() {
            return Optional.of(new Reducer.Sum());
        }
    }

    public static class TestWeightComputation extends TestPregelComputation {

        @Override
        public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
            return nodeValue * relationshipWeight;
        }
    }

    public static class TestSendTo implements PregelComputation<PregelConfig> {

        static final String KEY = "value";

        @Override
        public PregelSchema schema(PregelConfig config) {
            return new PregelSchema.Builder().add(KEY, ValueType.DOUBLE).build();
        }

        @Override
        public void compute(ComputeContext<PregelConfig> context, Messages messages) {
            if (context.nodeId() == 0) {
                var sum = StreamSupport.stream(messages.spliterator(), false).mapToDouble(d -> d).sum();
                context.setNodeValue(KEY, sum);
            } else {
                context.sendTo(0L, 1);
            }
        }
    }

    @ValueClass
    @Configuration
    @SuppressWarnings("immutables:subtype")
    public interface CompositeTestComputationConfig extends PregelConfig {
        String doubleProperty();

        String longProperty();
    }

    static class CompositeTestComputation implements PregelComputation<CompositeTestComputationConfig> {
        static final String LONG_KEY = "long";
        static final String DOUBLE_KEY = "double";
        static final String LONG_ARRAY_KEY = "long_array";
        static final String DOUBLE_ARRAY_KEY = "double_array";

        @Override
        public PregelSchema schema(CompositeTestComputationConfig config) {
            return new PregelSchema.Builder()
                .add(LONG_KEY, ValueType.LONG)
                .add(DOUBLE_KEY, ValueType.DOUBLE)
                .add(LONG_ARRAY_KEY, ValueType.LONG_ARRAY)
                .add(DOUBLE_ARRAY_KEY, ValueType.DOUBLE_ARRAY)
                .build();
        }

        @Override
        public void init(InitContext<CompositeTestComputationConfig> context) {
            long nodeId = context.nodeId();
            long longValue = context.nodeProperties(context.config().longProperty()).longValue(nodeId);
            double doubleValue = context.nodeProperties(context.config().doubleProperty()).doubleValue(nodeId);

            context.setNodeValue(LONG_KEY, longValue);
            context.setNodeValue(DOUBLE_KEY, doubleValue);
            context.setNodeValue(LONG_ARRAY_KEY, new long[]{longValue});
            context.setNodeValue(DOUBLE_ARRAY_KEY, new double[]{doubleValue});
        }

        @Override
        public void compute(
            ComputeContext<CompositeTestComputationConfig> context,
            Messages messages
        ) {
            if (!context.isInitialSuperstep()) {
                context.setNodeValue(LONG_KEY, context.longNodeValue(LONG_KEY) * 2);
                context.setNodeValue(DOUBLE_KEY, context.doubleNodeValue(DOUBLE_KEY) * 2);

                var longArray = context.longArrayNodeValue(LONG_ARRAY_KEY);
                context.setNodeValue(LONG_ARRAY_KEY, new long[]{longArray[0] * 2L});

                var doubleArray = context.doubleArrayNodeValue(DOUBLE_ARRAY_KEY);
                context.setNodeValue(DOUBLE_ARRAY_KEY, new double[]{doubleArray[0] * 2L});
            }
            context.sendToNeighbors(42.0);
        }
    }

    static class TestMasterCompute implements PregelComputation<PregelConfig> {

        private final int stopAtIteration;

        TestMasterCompute() {
            this(-1);
        }

        TestMasterCompute(int stopAtIteration) {
            this.stopAtIteration = stopAtIteration;
        }

        @Override
        public PregelSchema schema(PregelConfig config) {
            return new PregelSchema.Builder().add(KEY, ValueType.LONG).build();
        }

        @Override
        public void init(InitContext<PregelConfig> context) {
            context.setNodeValue(KEY, 0);
        }

        @Override
        public void compute(ComputeContext<PregelConfig> context, Messages messages) {

        }

        @Override
        public boolean masterCompute(MasterComputeContext<PregelConfig> context) {
            context.forEachNode(nodeId -> {
                context.setNodeValue(nodeId, KEY, context.longNodeValue(nodeId, KEY) + 1);
                return true;
            });
            return context.superstep() == stopAtIteration;
        }
    }

    static class TestEmptyMessageInInitialSuperstep implements PregelComputation<PregelConfig> {
        @Override
        public PregelSchema schema(PregelConfig config) {
            return new PregelSchema.Builder().build();
        }

        @Override
        public void compute(ComputeContext<PregelConfig> context, Messages messages) {
            if (context.isInitialSuperstep()) {
                context.sendToNeighbors(context.nodeId());
                // Nodes are processed sequentially per thread.
                // 0 is connected to 1 and 2; for asynchronous
                // computation, 1 and 2 will receive a message
                // from 0 in the first superstep.
                if (context.config().isAsynchronous() && context.nodeId() > 0) {
                    assertThat(messages).isNotEmpty();
                } else {
                    // In synchronous mode, no messages must be
                    // received in the same superstep.
                    assertThat(messages).isEmpty();
                }
            }
        }
    }

    @Test
    void throwIfBidirectionalWithoutInverseIndex() {
        ThrowableAssert.ThrowingCallable pregelCreate = () -> Pregel.create(
            graph,
            ImmutablePregelConfig.builder().maxIterations(4).build(),
            new Bidirectional(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        assertThatThrownBy(pregelCreate)
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("The Pregel algorithm Bidirectional requires inverse indexes for all configured relationships ['*']");
    }

    static class Bidirectional implements BidirectionalPregelComputation<PregelConfig> {
        @Override
        public PregelSchema schema(PregelConfig config) {
            return new PregelSchema.Builder().build();
        }

        @Override
        public void compute(BidirectionalComputeContext<PregelConfig> context, Messages messages) {

        }
    }
}
