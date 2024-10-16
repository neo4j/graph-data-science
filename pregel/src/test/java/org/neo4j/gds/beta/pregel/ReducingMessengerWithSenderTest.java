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

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.mem.MemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class ReducingMessengerWithSenderTest {

    @GdlGraph
    static final String GRAPH = """
        (a)-[{w: 0.42}]->(b)
        (a)-[{w: 0.12}]->(c)
        (b)-[{w: 0.84}]->(c)
        (c)-[{w: 0.23}]->(d)
        """;

    @Inject
    private TestGraph graph;

    static Stream<Arguments> expectedSenders() {
        return Stream.of(
            Arguments.of(new Reducer.Max(), Map.of(
                "a", "a",
                "b", "a",
                "c", "b",
                "d", "c"
            )),
            Arguments.of(new Reducer.Min(), Map.of(
                "a", "a",
                "b", "a",
                "c", "a",
                "d", "c"
            ))
        );
    }

    @ParameterizedTest
    @MethodSource("expectedSenders")
    void test(Reducer reducer, Map<String, String> expectedTargets, SoftAssertions softly) {
        var config = TrackingConfigImpl.builder()
            .relationshipWeightProperty("w")
            .maxIterations(10)
            .build();

        var result = Pregel.create(
            this.graph,
            config,
            new TestComputation(softly, reducer),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        ).run().nodeValues().longProperties(TestComputation.SENDER);

        expectedTargets.forEach((node, expectedSender) -> {
            softly.assertThat(result.get(graph.toMappedNodeId(node))).isEqualTo(graph.toMappedNodeId(expectedSender));
        });
    }

    static class TestComputation implements PregelComputation<TrackingConfig> {

        static final String SENDER = "sender";

        private final SoftAssertions softly;
        private final Reducer reducer;

        TestComputation(SoftAssertions softly, Reducer reducer) {
            this.softly = softly;
            this.reducer = reducer;
        }

        @Override
        public void init(InitContext<TrackingConfig> context) {
            context.setNodeValue(SENDER, context.nodeId());
        }

        @Override
        public void compute(ComputeContext<TrackingConfig> context, Messages messages) {
            if (context.isInitialSuperstep()) {
                context.sendToNeighbors(1.0);
            } else {
                softly.assertThat(messages.sender()).isPresent();
                context.setNodeValue(SENDER, messages.sender().orElseThrow());
                context.voteToHalt();
            }
        }

        @Override
        public Optional<Reducer> reducer() {
            return Optional.of(this.reducer);
        }

        @Override
        public double applyRelationshipWeight(double message, double relationshipWeight) {
            return message * relationshipWeight;
        }

        @Override
        public PregelSchema schema(TrackingConfig config) {
            return new PregelSchema.Builder()
                .add(SENDER, ValueType.LONG)
                .build();
        }

        @Override
        public MemoryEstimateDefinition estimateDefinition(boolean isAsynchronous) {
            return MemoryEstimations::empty;
        }
    }

    @Configuration("TrackingConfigImpl")
    interface TrackingConfig extends PregelConfig {
        @Override
        @Configuration.Ignore
        default boolean trackSender() {
            // this will trigger the Pregel framework to use a messenger that tracks the sender
            return true;
        }
    }
}
