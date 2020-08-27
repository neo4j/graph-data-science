/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.immutables.value.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.DOUBLE_ARRAY_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.DOUBLE_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.LONG_ARRAY_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.CompositeTestComputation.LONG_KEY;
import static org.neo4j.graphalgo.beta.pregel.PregelTest.TestPregelComputation.KEY;

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
    @MethodSource("configAndResult")
    <C extends PregelConfig> void sendsMessages(C config, PregelComputation<C> computation, double[] expected) {
        Pregel<C> pregelJob = Pregel.create(
            graph,
            config,
            computation,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var nodeValues = pregelJob.run().nodeValues();
        assertArrayEquals(expected, nodeValues.doubleProperties(KEY).toArray());
    }

    @Test
    void sendMessageToSpecificTarget() {
        var config = ImmutablePregelConfig.builder()
            .maxIterations(2)
            .concurrency(1)
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new TestSendTo(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
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

        var pregelJob = Pregel.create(
            graph,
            config,
            new CompositeTestComputation(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var result = pregelJob.run().nodeValues();

        assertEquals(46L, result.longValue(LONG_KEY, graph.toOriginalNodeId("alice")));
        assertEquals(84.0D, result.doubleValue(DOUBLE_KEY, graph.toOriginalNodeId("alice")));
        assertArrayEquals(new long[]{46L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toOriginalNodeId("alice")));
        assertArrayEquals(new double[]{84.0D}, result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toOriginalNodeId("alice")));

        assertEquals(48L, result.longValue(LONG_KEY, graph.toOriginalNodeId("bob")));
        assertEquals(86.0D, result.doubleValue(DOUBLE_KEY, graph.toOriginalNodeId("bob")));
        assertArrayEquals(new long[]{48L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toOriginalNodeId("bob")));
        assertArrayEquals(new double[]{86.0D}, result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toOriginalNodeId("bob")));

        assertEquals(50L, result.longValue(LONG_KEY, graph.toOriginalNodeId("eve")));
        assertEquals(88.0D, result.doubleValue(DOUBLE_KEY, graph.toOriginalNodeId("eve")));
        assertArrayEquals(new long[]{50L}, result.longArrayValue(LONG_ARRAY_KEY, graph.toOriginalNodeId("eve")));
        assertArrayEquals(new double[]{88.0D}, result.doubleArrayValue(DOUBLE_ARRAY_KEY, graph.toOriginalNodeId("eve")));
    }

    @Test
    void memoryEstimation() {
        var dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(10_000)
            .maxRelCount(100_000)
            .build();

        assertEquals(
            MemoryRange.of(4_884_064L),
            Pregel.memoryEstimation().estimate(dimensions, 1).memoryUsage()
        );

        assertEquals(
            MemoryRange.of(4_896_448L),
            Pregel.memoryEstimation().estimate(dimensions, 10).memoryUsage()
        );
    }

    static Stream<Arguments> configAndResult() {
        return Stream.of(
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).build(),
                new TestPregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).relationshipWeightProperty("prop").build(),
                new TestPregelComputation(),
                new double[]{0.0, 1.0, 1.0}
            ),
            Arguments.of(
                ImmutablePregelConfig.builder().maxIterations(2).relationshipWeightProperty("prop").build(),
                new TestWeightComputation(),
                new double[]{0.0, 2.0, 1.0}
            )
        );
    }

    public static class TestPregelComputation implements PregelComputation<PregelConfig> {

        static final String KEY = "value";

        @Override
        public Pregel.NodeSchema nodeSchema() {
            return new NodeSchemaBuilder()
                .putElement(KEY, ValueType.DOUBLE)
                .build();
        }

        @Override
        public void compute(PregelContext.ComputeContext<PregelConfig> context, Pregel.Messages messages) {
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

    public static class TestWeightComputation extends TestPregelComputation {

        @Override
        public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
            return nodeValue * relationshipWeight;
        }
    }

    public static class TestSendTo implements PregelComputation<PregelConfig> {

        static final String KEY = "value";

        @Override
        public Pregel.NodeSchema nodeSchema() {
            return new NodeSchemaBuilder().putElement(KEY, ValueType.DOUBLE).build();
        }

        @Override
        public void compute(PregelContext.ComputeContext<PregelConfig> context, Pregel.Messages messages) {
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
        @Value
        String doubleProperty();

        @Value
        String longProperty();
    }

    static class CompositeTestComputation implements PregelComputation<CompositeTestComputationConfig> {
        static final String LONG_KEY = "long";
        static final String DOUBLE_KEY = "double";
        static final String LONG_ARRAY_KEY = "long_array";
        static final String DOUBLE_ARRAY_KEY = "double_array";

        @Override
        public Pregel.NodeSchema nodeSchema() {
            return new NodeSchemaBuilder()
                .putElement(LONG_KEY, ValueType.LONG)
                .putElement(DOUBLE_KEY, ValueType.DOUBLE)
                .putElement(LONG_ARRAY_KEY, ValueType.LONG_ARRAY)
                .putElement(DOUBLE_ARRAY_KEY, ValueType.DOUBLE_ARRAY)
                .build();
        }

        @Override
        public void init(PregelContext.InitContext<CompositeTestComputationConfig> context) {
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
            PregelContext.ComputeContext<CompositeTestComputationConfig> context,
            Pregel.Messages messages
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
}
