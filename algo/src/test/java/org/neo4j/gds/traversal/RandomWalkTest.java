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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.PerDatabaseTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
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
    private TestGraph graph;

    @Test
    void testWithDefaultConfig() {
        var walkParameters = new WalkParameters(10, 80, 1.0, 1.0);
        var randomWalk = RandomWalk.create(
            graph,
            4,
            walkParameters,
            List.of(),
            1000,
            Optional.empty(),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        );

        List<long[]> result = randomWalk.compute().collect(Collectors.toList());

        int expectedNumberOfWalks = walkParameters.walksPerNode * 3;
        assertEquals(expectedNumberOfWalks, result.size());
        long nodeZero = graph.toMappedNodeId("a");
        long[] walkForNodeZero = result
            .stream()
            .filter(arr -> arr[0] == nodeZero)
            .findFirst()
            .orElse(new long[0]);
        int expectedStepsInWalkForNode0 = walkParameters.walkLength;
        assertEquals(expectedStepsInWalkForNode0, walkForNodeZero.length);
    }

    @Test
    void shouldBeDeterministic() {
        var concurrency = 4;
        var randomSeed = Optional.of(42L);
        var walkParameters = new WalkParameters(10, 80, 1.0, 1.0);
        var sourceNodes = List.<Long>of();
        var walkBufferSize = 1000;

        var firstResult = RandomWalk.create(
            graph,
            concurrency,
            walkParameters,
            sourceNodes,
            walkBufferSize,
            randomSeed,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        ).compute().collect(Collectors.toList());

        var secondResult = RandomWalk.create(
            graph,
            concurrency,
            walkParameters,
            sourceNodes,
            walkBufferSize,
            randomSeed,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        ).compute().collect(Collectors.toList());

        var firstResultAsSet = new TreeSet<long[]>(Arrays::compare);
        firstResultAsSet.addAll(firstResult);
        assertThat(firstResultAsSet).hasSize(firstResult.size());

        var secondResultAsSet = new TreeSet<long[]>(Arrays::compare);
        secondResultAsSet.addAll(secondResult);
        assertThat(secondResultAsSet).hasSize(secondResult.size());

        assertThat(firstResultAsSet).isEqualTo(secondResultAsSet);
    }

    @Test
    void testSampleFromMultipleRelationshipTypes() {
        WalkParameters walkParameters = new WalkParameters(10, 80, 1.0, 1.0);
        RandomWalk randomWalk = RandomWalk.create(
            graph,
            4,
            walkParameters,
            List.of(),
            1000,
            Optional.empty(),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        );

        int expectedNumberOfWalks = walkParameters.walksPerNode * 3;
        List<long[]> result = randomWalk.compute().collect(Collectors.toList());
        assertEquals(expectedNumberOfWalks, result.size());
        long nodeZero = graph.toMappedNodeId("a");
        long[] walkForNodeZero = result
            .stream()
            .filter(arr -> arr[0] == nodeZero)
            .findFirst()
            .orElse(new long[0]);
        int expectedStepsInWalkForNode0 = walkParameters.walkLength;
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

        var walkParameters = new WalkParameters(100, 10, 0.01, 1.0);

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            4,
            walkParameters,
            List.of(),
            1000,
            Optional.of(42L),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
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

        var walkParameters = new WalkParameters(1000, 10, 0.1, 100_000.0);

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            4,
            walkParameters,
            List.of(),
            1000,
            Optional.of(87L),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
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

        var walkParameters = new WalkParameters(1, 1000, 1.0, 1.0);

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            1,
            walkParameters,
            List.of(),
            100,
            Optional.of(23L),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
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

        var walkParameters = new WalkParameters(1, 1000, 1.0, 1.0);

        assertThatThrownBy(
            () -> RandomWalk.create(
                graph,
                1,
                walkParameters,
                List.of(),
                100,
                Optional.of(23L),
                ProgressTracker.NULL_TRACKER,
                DefaultPool.INSTANCE
            )
        ).isInstanceOf(RuntimeException.class)
            .hasMessage(
                formatWithLocale(
                    "Found an invalid relationship weight between nodes `0` and `1` with the property value of `%f`." +
                    " RandomWalk only supports non-negative weights.", invalidWeight));
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

        int walksPerNode = 1;
        int walkLength = 10;
        var walkParameters = new WalkParameters(walksPerNode, walkLength, 1.0, 1.0);

        var randomWalk = RandomWalk.create(
            graph,
            4,
            walkParameters,
            List.of(),
            100,
            Optional.of(23L),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        );

        assertThat(randomWalk.compute().collect(Collectors.toList()))
            .matches(walks -> walks.size() <= nodeCount * walksPerNode)
            .allMatch(walk -> walk.length <= walkLength);
    }

    @Test
    void testWithConfiguredOffsetStartNodes() {
        var graph = TestSupport.fromGdl(GDL, 42);

        var aOriginalId = graph.toOriginalNodeId("a");
        var bOriginalId = graph.toOriginalNodeId("b");

        var aInternalId = graph.toMappedNodeId(aOriginalId);
        var bInternalId = graph.toMappedNodeId(bOriginalId);

        var sourceNodes = List.of(aOriginalId, bOriginalId);
        var walkParameters = new WalkParameters(1, 3, 1.0, 1.0);

        var randomWalk = RandomWalk.create(
            graph,
            4,
            walkParameters,
            sourceNodes,
            1000,
            Optional.empty(),
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE
        );

        assertThat(randomWalk.compute().collect(Collectors.toList()))
            .matches(walks -> walks.size() == 2)
            .anyMatch(walk -> walk[0] == aInternalId)
            .anyMatch(walk -> walk[0] == bInternalId);
    }

    /**
     * Ensure that when termination flag is set externally, we terminate the walk
     */
    @Test
    void testSetTerminationFlagAndMultipleRuns() {
        for (int i = 0; i < 3; i++) {

            var randomWalk = RandomWalk.create(
                    graph,
                    4,
                    new WalkParameters(10, 80, 1.0, 1.0),
                    List.of(),
                    1,
                    Optional.empty(),
                    ProgressTracker.NULL_TRACKER,
                    DefaultPool.INSTANCE
            );

            var stream = randomWalk.compute();
            long count = stream.limit(10).count();

            randomWalk.setTerminationFlag(new TerminationFlag() {
                @Override
                public boolean running() {
                    return false;
                }
            });

            assertEquals(10, count);
        }
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

        @GdlGraph(graphNamePrefix = "weighted")
        public static final String WEIGHTED_GDL =
            "CREATE " +
                "  (a:Node)" +
                ", (b:Node)" +
                ", (c:Node)" +
                ", (d:Node)" +
                ", (e:Node)" +
                ", (f:Node)" +
                ", (a)-[:REL{w:10}]->(b)" +
                ", (a)-[:REL{w:10}]->(c)" +
                ", (a)-[:REL{w:5}]->(d)" +
                ", (b)-[:REL{w:5}]->(a)" +
                ", (b)-[:REL{w:5}]->(e)" +
                ", (c)-[:REL{w:5}]->(a)" +
                ", (c)-[:REL{w:5}]->(d)" +
                ", (c)-[:REL{w:5}]->(e)" +
                ", (d)-[:REL{w:5}]->(a)" +
                ", (d)-[:REL{w:5}]->(c)" +
                ", (d)-[:REL{w:5}]->(e)" +
                ", (e)-[:REL{w:5}]->(a)";

        @Inject
        private TestGraph weightedGraph;

        @Test
        void progressLogging() throws InterruptedException {

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
            var taskStore = new PerDatabaseTaskStore();

            var pt = new TestProgressTracker(
                fact.progressTask(graph, config),
                log,
                config.concurrency(),
                TaskRegistryFactory.local("rw", taskStore)
            );

            RandomWalk randomWalk = fact.build(graph, config, pt);

            assertThatNoException().isThrownBy(() -> {
                var randomWalksStream = randomWalk.compute();
                // Make sure to consume the stream...
                assertThat(randomWalksStream).hasSize(5000);
            });

            awaitEmptyTaskStore(taskStore);

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

        @Test
        void shouldLogProgressOnWeightedGraph() throws InterruptedException {

            var config = RandomWalkStreamConfigImpl.builder()
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
            var taskStore = new PerDatabaseTaskStore();

            var pt = new TestProgressTracker(
                fact.progressTask(weightedGraph, config),
                log,
                config.concurrency(),
                TaskRegistryFactory.local("rw", taskStore)
            );

            RandomWalk randomWalk = fact.build(weightedGraph, config, pt);

            assertThatNoException().isThrownBy(() -> {
                var randomWalksStream = randomWalk.compute();
                // Make sure to consume the stream...
                assertThat(randomWalksStream).hasSize(5000);
            });

            awaitEmptyTaskStore(taskStore);

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "RandomWalk :: Start",
                    "RandomWalk :: DegreeCentrality :: Start",
                    "RandomWalk :: DegreeCentrality 100%",
                    "RandomWalk :: DegreeCentrality :: Finished",
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

        @Test
        void shouldLeaveNoTasksBehind() {
            var config = ImmutableRandomWalkStreamConfig.builder().build();
            var factory = new RandomWalkAlgorithmFactory<RandomWalkStreamConfig>();
            var taskStore = new PerDatabaseTaskStore();
            var progressTracker = new TaskProgressTracker(
                factory.progressTask(
                    graph,
                    config
                ),
                Neo4jProxy.testLog(),
                4,
                TaskRegistryFactory.local("rw", taskStore)
            );

            // run the algorithm and consume the result stream
            factory.build(graph, config, progressTracker).compute().count();

            awaitEmptyTaskStore(taskStore);

            // the task store should now be empty
            assertThat(taskStore.isEmpty()).isTrue();
        }

        private void awaitEmptyTaskStore(PerDatabaseTaskStore taskStore) {
            // there is a race condition between the thread consuming the result,
            // and the thread scheduled to end the last subtask
            long timeoutInSeconds = 5 * (TestSupport.CI ? 5 : 1);
            var deadline = Instant.now().plus(timeoutInSeconds, ChronoUnit.SECONDS);

            // On my machine (TM) with 1000 iterations this never fails and each run is 10ish or 100ish ms
            while (Instant.now().isBefore(deadline)) {
                if (taskStore.isEmpty()) {
                    break;
                }

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }
}
