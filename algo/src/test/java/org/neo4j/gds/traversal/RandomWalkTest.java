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
package org.neo4j.gds.traversal;

import org.assertj.core.data.Offset;
import org.assertj.core.data.Percentage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.node2vec.ImmutableNode2VecStreamConfig;
import org.neo4j.gds.embeddings.node2vec.Node2VecStreamConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class RandomWalkTest {

    @GdlGraph
    private static final String GDL =
        "CREATE" +
        "  (a:Node1)" +
        ", (b:Node1)" +
        ", (c:Node2)" +
        ", (d:Isolated)" +
        ", (e:Isolated)" +
        ", (a)-[:REL1]->(b)" +
        ", (b)-[:REL1]->(a)" +
        ", (a)-[:REL1]->(c)" +
        ", (c)-[:REL2]->(a)" +
        ", (b)-[:REL2]->(c)" +
        ", (c)-[:REL2]->(b)";

    @Inject
    private CSRGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void testWithDefaultConfig() {
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder().build();

        var randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        List<long[]> result = randomWalk.compute().collect(Collectors.toList());

        int expectedNumberOfWalks = config.walksPerNode() * 3;
        assertEquals(expectedNumberOfWalks, result.size());
        long[] walkForNodeZero = result
            .stream()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .findFirst()
            .orElse(new long[0]);
        int expectedStepsInWalkForNode0 = config.walkLength();
        assertEquals(expectedStepsInWalkForNode0, walkForNodeZero.length);
    }

    @Test
    void shouldBeDeterministic() {
        var config = ImmutableNode2VecStreamConfig.builder().concurrency(4).randomSeed(42L).build();

        var firstResult = runRandomWalkSeeded(config, graph);
        var secondResult = runRandomWalkSeeded(config, graph);

        var firstResultAsSet = new TreeSet<long[]>(Arrays::compare);
        firstResultAsSet.addAll(firstResult);
        assertThat(firstResultAsSet).hasSize(firstResult.size());

        var secondResultAsSet = new TreeSet<long[]>(Arrays::compare);
        secondResultAsSet.addAll(secondResult);
        assertThat(secondResultAsSet).hasSize(secondResult.size());

        assertThat(firstResultAsSet).isEqualTo(secondResultAsSet);
    }

    @NotNull
    private List<long[]> runRandomWalkSeeded(Node2VecStreamConfig config, Graph graph) {
        var randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        return randomWalk.compute().collect(Collectors.toList());
    }

    @Test
    void testSampleFromMultipleRelationshipTypes() {
        Node2VecStreamConfig config = ImmutableNode2VecStreamConfig.builder().build();
        RandomWalk randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        int expectedNumberOfWalks = config.walksPerNode() * 3;
        List<long[]> result = randomWalk.compute().collect(Collectors.toList());
        assertEquals(expectedNumberOfWalks, result.size());
        long[] walkForNodeZero = result
            .stream()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .findFirst()
            .orElse(new long[0]);
        int expectedStepsInWalkForNode0 = config.walkLength();
        assertEquals(expectedStepsInWalkForNode0, walkForNodeZero.length);
    }

    @Test
    void returnFactorShouldMakeWalksIncludeStartNodeMoreOften() {
        var graph = fromGdl("CREATE (a:Node)" +
                 ", (a)-[:REL]->(b:Node)-[:REL]->(a)" +
                 ", (b)-[:REL]->(c:Node)-[:REL]->(a)" +
                 ", (c)-[:REL]->(d:Node)-[:REL]->(a)" +
                 ", (d)-[:REL]->(e:Node)-[:REL]->(a)" +
                 ", (e)-[:REL]->(f:Node)-[:REL]->(a)" +
                 ", (f)-[:REL]->(g:Node)-[:REL]->(a)" +
                 ", (g)-[:REL]->(h:Node)-[:REL]->(a)");

        var config = ImmutableNode2VecStreamConfig.builder()
            .walkLength(10)
            .concurrency(4)
            .walksPerNode(100)
            .walkBufferSize(1000)
            .returnFactor(0.01)
            .inOutFactor(1)
            .randomSeed(42L)
            .build();

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        var nodeCounter = new HashMap<Long, Long>();
        randomWalk
            .compute()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .forEach(arr -> Arrays.stream(arr).forEach(n -> {
                    long neo4jId = graph.toOriginalNodeId(n);
                    nodeCounter.merge(neo4jId, 1L, Long::sum);
                })
            );

        // (a) and (b) have similar occurrences, since from (a) the only reachable node is (b)
        assertThat(Math.abs(nodeCounter.get(0L) - nodeCounter.get(1L)) <= 100)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();

        // all other nodes should occur far less often because of the high return probability
        assertThat(nodeCounter.get(0L) > nodeCounter.getOrDefault(2L, 0L) * 40)
            .withFailMessage("occurrences: %s", nodeCounter)
            .isTrue();
    }

    @Test
    void largeInOutFactorShouldMakeTheWalkKeepTheSameDistance() {
        var graph = fromGdl("CREATE " +
                 "  (a:Node)" +
                 ", (b:Node)" +
                 ", (c:Node)" +
                 ", (d:Node)" +
                 ", (e:Node)" +
                 ", (f:Node)" +
                 ", (a)-[:REL]->(b)" +
                 ", (a)-[:REL]->(c)" +
                 ", (a)-[:REL]->(d)" +
                 ", (b)-[:REL]->(a)" +
                 ", (b)-[:REL]->(e)" +
                 ", (c)-[:REL]->(a)" +
                 ", (c)-[:REL]->(d)" +
                 ", (c)-[:REL]->(e)" +
                 ", (d)-[:REL]->(a)" +
                 ", (d)-[:REL]->(c)" +
                 ", (d)-[:REL]->(e)" +
                 ", (e)-[:REL]->(a)");

        var config = ImmutableNode2VecStreamConfig.builder()
            .walkLength(10)
            .concurrency(4)
            .walksPerNode(1000)
            .walkBufferSize(1000)
            .returnFactor(0.1)
            .inOutFactor(100000)
            .randomSeed(87L)
            .build();

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        var nodeCounter = new HashMap<Long, Long>();
        randomWalk
            .compute()
            .filter(arr -> graph.toOriginalNodeId(arr[0]) == 0)
            .forEach(arr -> Arrays.stream(arr).forEach(n -> {
                    long neo4jId = graph.toOriginalNodeId(n);
                    nodeCounter.merge(neo4jId, 1L, Long::sum);
                })
            );

        // (a), (b), (c), (d) should be much more common than (e)
        assertThat(nodeCounter.get(0L)).isNotCloseTo(nodeCounter.get(4L), Offset.offset(4000L));
        assertThat(nodeCounter.get(1L)).isNotCloseTo(nodeCounter.get(4L), Offset.offset(1200L));
        assertThat(nodeCounter.get(2L)).isNotCloseTo(nodeCounter.get(4L), Offset.offset(1200L));
        assertThat(nodeCounter.get(3L)).isNotCloseTo(nodeCounter.get(4L), Offset.offset(1200L));

        assertThat(nodeCounter.get(4L)).isLessThan(100);
    }


    @Test
    void shouldRespectRelationshipWeights() {
        var graph = fromGdl(
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (a)-[:REL {weight: 100.0}]->(b)" +
            ", (a)-[:REL {weight: 1.0}]->(c)" +
            ", (b)-[:REL {weight: 1.0}]->(a)" +
            ", (c)-[:REL {weight: 1.0}]->(a)"
        );

        var config = ImmutableNode2VecStreamConfig.builder()
            .walkLength(1000)
            .concurrency(1)
            .walksPerNode(1)
            .walkBufferSize(100)
            .returnFactor(1)
            .inOutFactor(1)
            .randomSeed(23L)
            .build();

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        var nodeCounter = new HashMap<Long, Long>();
        randomWalk
            .compute()
            .forEach(arr -> Arrays.stream(arr).forEach(n -> {
                    long neo4jId = graph.toOriginalNodeId(n);
                    nodeCounter.merge(neo4jId, 1L, Long::sum);
                })
            );

        assertThat(nodeCounter.get(0L)).isCloseTo(1500, Percentage.withPercentage(10));
        assertThat(nodeCounter.get(1L)).isCloseTo(1500, Percentage.withPercentage(10));
        assertThat(nodeCounter.get(2L)).isCloseTo(1L, Offset.offset(50L));
    }

    @ParameterizedTest
    @ValueSource(doubles = {-1, Double.NaN})
    void failOnInvalidRelationshipWeights(double invalidWeight) {
        var graph = fromGdl(formatWithLocale("(a)-[:REL {weight: %f}]->(b)", invalidWeight));

        var config = ImmutableNode2VecStreamConfig.builder()
            .walkLength(1000)
            .concurrency(1)
            .walksPerNode(1)
            .walkBufferSize(100)
            .returnFactor(1)
            .inOutFactor(1)
            .randomSeed(23L)
            .build();

        assertThatThrownBy(
            () -> RandomWalk.create(
                graph,
                config,
                ProgressTracker.NULL_TRACKER,
                Pools.DEFAULT
            )
        ).isInstanceOf(RuntimeException.class)
            .hasMessage(
                formatWithLocale(
                    "Found an invalid relationship weight between nodes `0` and `1` with the property value of `%f`." +
                    " Node2Vec only supports non-negative weights.", invalidWeight));
    }

    @Test
    void parallelWeighted() {
        int nodeCount = 20_000;
        var graph = new RandomGraphGeneratorBuilder()
            .nodeCount(nodeCount)
            .averageDegree(10)
            .relationshipPropertyProducer(PropertyProducer.randomDouble("weight", 0, 0.1))
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(24L)
            .build()
            .generate();

        var config = ImmutableNode2VecStreamConfig.builder()
            .walkLength(10)
            .concurrency(4)
            .walksPerNode(1)
            .walkBufferSize(100)
            .returnFactor(1)
            .inOutFactor(1)
            .randomSeed(23L)
            .build();

        var randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        assertThat(randomWalk.compute().collect(Collectors.toList()))
            .matches(walks -> walks.size() <= nodeCount * config.walksPerNode())
            .allMatch(walk -> walk.length <= config.walkLength());
    }

    @Test
    void testWithConfiguredOffsetStartNodes() {
        var graph = TestSupport.fromGdl(GDL, 42);

        var aOriginalId = graph.toOriginalNodeId("a");
        var bOriginalId = graph.toOriginalNodeId("b");

        var aInternalId = graph.toMappedNodeId(aOriginalId);
        var bInternalId = graph.toMappedNodeId(bOriginalId);

        var config = ImmutableNode2VecStreamConfig.builder()
            .sourceNodes(List.of(aOriginalId, bOriginalId))
            .walkLength(3)
            .concurrency(4)
            .walksPerNode(1)
            .build();

        var randomWalk = RandomWalk.create(
            graph,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        );

        assertThat(randomWalk.compute().collect(Collectors.toList()))
            .matches(walks -> walks.size() == 2)
            .anyMatch(walk -> walk[0] == aInternalId)
            .anyMatch(walk -> walk[0] == bInternalId);
    }

    @Nested
    class ProgressTracking {

        @GdlGraph
        public static final String GDL =
            "CREATE " +
            "  (a:Node)" +
            ", (b:Node)" +
            ", (c:Node)" +
            ", (d:Node)" +
            ", (e:Node)" +
            ", (f:Node)" +
            ", (a)-[:REL]->(b)" +
            ", (a)-[:REL]->(c)" +
            ", (a)-[:REL]->(d)" +
            ", (b)-[:REL]->(a)" +
            ", (b)-[:REL]->(e)" +
            ", (c)-[:REL]->(a)" +
            ", (c)-[:REL]->(d)" +
            ", (c)-[:REL]->(e)" +
            ", (d)-[:REL]->(a)" +
            ", (d)-[:REL]->(c)" +
            ", (d)-[:REL]->(e)" +
            ", (e)-[:REL]->(a)";

        @Inject
        TestGraph graph;

        @Test
        void progressLogging() {

            var config = ImmutableRandomWalkStreamConfig.builder()
                .walkLength(10)
                .concurrency(4)
                .walksPerNode(1000)
                .walkBufferSize(1000)
                .returnFactor(0.1)
                .inOutFactor(100000)
                .randomSeed(87L)
                .build();

            var fact = new RandomWalkAlgorithmFactory<RandomWalkStreamConfig>();
            var log = Neo4jProxy.testLog();
            var pt = new TestProgressTracker(
                fact.progressTask(graph, config),
                log,
                config.concurrency(),
                EmptyTaskRegistryFactory.INSTANCE
            );

            RandomWalk randomWalk = fact.build(graph, config, pt);

            assertThatNoException().isThrownBy(() -> {
                var randomWalksStream = randomWalk.compute();
                // Make sure to consume the stream...
                assertThat(randomWalksStream).hasSize(5000);
            });

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "RandomWalk :: Start",
                    "RandomWalk :: create walks :: Start",
                    "RandomWalk :: create walks 16%",
                    "RandomWalk :: create walks 33%",
                    "RandomWalk :: create walks 50%",
                    "RandomWalk :: create walks 66%",
                    "RandomWalk :: create walks 83%",
                    "RandomWalk :: create walks 100%",
                    "RandomWalk :: create walks :: Finished",
                    "RandomWalk :: Finished"
                );
        }

    }
}
