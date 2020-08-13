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
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;

import java.util.Queue;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@GdlExtension
class PregelTest {

    @GdlGraph
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (alice:Node { a_seed: 42.0, b_seed: 23 })" +
        ", (bob:Node { a_seed: 43.0, b_seed: 24 })" +
        ", (eve:Node { a_seed: 44.0, b_seed: 25 })" +
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
            10,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeDoubleArray nodeValues = pregelJob.run().nodeValues();
        assertArrayEquals(expected, nodeValues.toArray());
    }

    @Test
    void compositeNodeValueTest() {
        var config = ImmutableCompositeTestComputationConfig.builder()
            .maxIterations(2)
            .concurrency(1)
            .aProperty("a_seed")
            .bProperty("b_seed")
            .build();

        var pregelJob = Pregel.create(
            graph,
            config,
            new CompositeTestComputation(),
            10,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        var result = pregelJob.run().compositeNodeValues();

        assertEquals(84.0D, result.doubleValue("a", graph.toOriginalNodeId("alice")));
        assertEquals(46L, result.longValue("b", graph.toOriginalNodeId("alice")));

        assertEquals(86.0D, result.doubleValue("a", graph.toOriginalNodeId("bob")));
        assertEquals(48L, result.longValue("b", graph.toOriginalNodeId("bob")));

        assertEquals(88.0D, result.doubleValue("a", graph.toOriginalNodeId("eve")));
        assertEquals(50L, result.longValue("b", graph.toOriginalNodeId("eve")));
    }

    @Test
    void memoryEstimation() {
        var dimensions = ImmutableGraphDimensions.builder()
            .nodeCount(10_000)
            .maxRelCount(100_000)
            .build();

        assertEquals(
            MemoryRange.of(4_724_064L),
            Pregel.memoryEstimation().estimate(dimensions, 1).memoryUsage()
        );

        assertEquals(
            MemoryRange.of(4_736_376L),
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

        @Override
        public void compute(PregelContext<PregelConfig> pregel, long nodeId, Queue<Double> messages) {
            if (pregel.isInitialSuperstep()) {
                pregel.setNodeValue(nodeId, 0.0);
                pregel.sendMessages(nodeId, 1.0);
            } else if (messages != null) {
                double messageSum = 0.0;
                Double nextMessage;
                while (!(nextMessage = messages.poll()).isNaN()) {
                    messageSum += nextMessage.longValue();
                }

                pregel.setNodeValue(nodeId, messageSum);
            }
            pregel.voteToHalt(nodeId);
        }
    }

    public static class TestWeightComputation extends TestPregelComputation {

        @Override
        public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
            return nodeValue * relationshipWeight;
        }
    }

    @ValueClass
    @Configuration
    @SuppressWarnings("immutables:subtype")
    public interface CompositeTestComputationConfig extends PregelConfig {
        @Value
        String aProperty();

        @Value
        String bProperty();
    }

    private static class CompositeTestComputation implements PregelComputation<CompositeTestComputationConfig> {

        @Override
        public Pregel.NodeSchema nodeSchema() {
            return new NodeSchemaBuilder()
                .putElement("a", ValueType.DOUBLE)
                .putElement("b", ValueType.LONG)
                .build();
        }

        @Override
        public void init(PregelContext<CompositeTestComputationConfig> context, long nodeId) {
            context.setNodeValue("a", nodeId, context.nodeProperties(context.getConfig().aProperty()).doubleValue(nodeId));
            context.setNodeValue("b", nodeId, context.nodeProperties(context.getConfig().bProperty()).longValue(nodeId));
        }

        @Override
        public void compute(PregelContext<CompositeTestComputationConfig> context, long nodeId, Queue<Double> messages) {
            if (!context.isInitialSuperstep()) {
                context.setNodeValue("a", nodeId, context.doubleNodeValue("a", nodeId) * 2);
                context.setNodeValue("b", nodeId, context.longNodeValue("b", nodeId) * 2);
            }
            context.sendMessages(nodeId, 42.0);
        }
    }
}
