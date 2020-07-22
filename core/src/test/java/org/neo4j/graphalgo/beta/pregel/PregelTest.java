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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Queue;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@GdlExtension
class PregelTest {

    @GdlGraph
    private static final String TEST_GRAPH =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (a)-[:REL {prop: 2.0}]->(b)" +
        ", (a)-[:REL {prop: 1.0}]->(c)";

    @Inject
    private Graph graph;

    @ParameterizedTest
    @MethodSource("configAndResult")
    <C extends PregelConfig> void sendsMessages(C config, PregelComputation<C> computation, double[] expected) {
        Pregel<C> pregelJob = Pregel.withDefaultNodeValues(
            graph,
            config,
            computation,
            10,
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        );

        HugeDoubleArray nodeValues = pregelJob.run();
        assertArrayEquals(expected, nodeValues.toArray());
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
            if (pregel.isInitialSuperStep()) {
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
}
