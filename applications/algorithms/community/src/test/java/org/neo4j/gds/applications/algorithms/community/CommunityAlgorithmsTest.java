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
package org.neo4j.gds.applications.algorithms.community;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.conductance.ConductanceStreamConfigImpl;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfigImpl;
import org.neo4j.gds.kcore.KCoreDecompositionStreamConfigImpl;
import org.neo4j.gds.kmeans.KmeansStreamConfigImpl;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfigImpl;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

final class CommunityAlgorithmsTest {

    private CommunityAlgorithmsTest() {}

    @GdlExtension
    @Nested
    class KCore {

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (z:node)," +
                "  (a:node)," +
                "  (b:node)," +
                "  (c:node)," +
                "  (d:node)," +
                "  (e:node)," +
                "  (f:node)," +
                "  (g:node)," +
                "  (h:node)," +

                "(a)-[:R]->(b)," +
                "(b)-[:R]->(c)," +
                "(c)-[:R]->(d)," +
                "(d)-[:R]->(e)," +
                "(e)-[:R]->(f)," +
                "(f)-[:R]->(g)," +
                "(g)-[:R]->(h)," +
                "(h)-[:R]->(c)";


        @Inject
        private TestGraph graph;


        @Test
        void shouldLogProgressForKcore() {
            var config = KCoreDecompositionStreamConfigImpl.builder().build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(4,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.kCore(graph, config);

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "KCoreDecomposition :: Start",
                    "KCoreDecomposition 11%",
                    "KCoreDecomposition 33%",
                    "KCoreDecomposition 100%",
                    "KCoreDecomposition :: Finished"
                );
        }
    }

    @GdlExtension
    @Nested
    class Conductance{

        @GdlGraph(orientation = Orientation.NATURAL)
        private static final String TEST_GRAPH =
            "CREATE" +
                "  (a:Label1 { community: 0 })" +
                ", (b:Label1 { community: 0 })" +
                ", (c:Label1 { community: 0 })" +
                ", (d:Label1 { community: 1 })" +
                ", (e:Label1 { community: 1 })" +
                ", (f:Label1 { community: 1 })" +
                ", (g:Label1 { community: 1 })" +
                ", (h:Label1 { community: -1 })" +

                ", (a)-[:TYPE1 {weight: 81.0}]->(b)" +
                ", (a)-[:TYPE1 {weight: 7.0}]->(d)" +
                ", (b)-[:TYPE1 {weight: 1.0}]->(d)" +
                ", (b)-[:TYPE1 {weight: 1.0}]->(g)" +
                ", (b)-[:TYPE1 {weight: 3.0}]->(h)" +
                ", (c)-[:TYPE1 {weight: 45.0}]->(b)" +
                ", (c)-[:TYPE1 {weight: 3.0}]->(e)" +
                ", (d)-[:TYPE1 {weight: 3.0}]->(c)" +
                ", (e)-[:TYPE1 {weight: 1.0}]->(b)" +
                ", (f)-[:TYPE1 {weight: 3.0}]->(a)" +
                ", (g)-[:TYPE1 {weight: 4.0}]->(c)" +
                ", (g)-[:TYPE1 {weight: 999.0}]->(g)" +
                ", (h)-[:TYPE1 {weight: 2.0}]->(a)";

        @Inject
        private TestGraph graph;

        @Test
        void logProgress() {
            var config = ConductanceStreamConfigImpl.builder().concurrency(1).communityProperty("community").build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);
            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.conductance(graph, config);

            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "Conductance :: Start",
                    "Conductance :: count relationships :: Start",
                    "Conductance :: count relationships 100%",
                    "Conductance :: count relationships :: Finished",
                    "Conductance :: accumulate counts :: Start",
                    "Conductance :: accumulate counts 100%",
                    "Conductance :: accumulate counts :: Finished",
                    "Conductance :: perform conductance computations :: Start",
                    "Conductance :: perform conductance computations 100%",
                    "Conductance :: perform conductance computations :: Finished",
                    "Conductance :: Finished"
                );
        }

    }

    @Nested
    class K1Coloring{

        @Test
        void shouldLogProgress(){
            var graph = RandomGraphGenerator.builder()
                .nodeCount(100)
                .averageDegree(10)
                .relationshipDistribution(RelationshipDistribution.UNIFORM)
                .seed(42L)
                .build()
                .generate();


            var config = K1ColoringStreamConfigImpl.builder()
                .concurrency(new Concurrency(4))
                .maxIterations(10)
                .build();

            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);
            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
           var result=algorithms.k1Coloring(graph,config);

            assertTrue(log.containsMessage(TestLog.INFO, ":: Start"));
            LongStream.range(1, result.ranIterations() + 1).forEach(iteration ->
                assertThat(log.getMessages(TestLog.INFO)).anyMatch(message -> {
                    var expected = "%d of %d".formatted(iteration, config.maxIterations());
                    return message.contains(expected);
                })
            );
            assertTrue(log.containsMessage(TestLog.INFO, ":: Finished"));
        }

    }

    @Nested
    @GdlExtension
    class  Kmeans{

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
                "  (a {  kmeans: [1.0, 1.0], fail: [1.0]} )" +
                "  (b {  kmeans: [1.0, 2.0]} )" +
                "  (c {  kmeans: [102.0, 100.0], fail:[1.0]} )" +
                "  (d {  kmeans: [100.0, 102.0]} )";
        @Inject
        private Graph graph;

        @Test
        void progressTracking() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .maxIterations(5)
                .numberOfRestarts(1)
                .k(2)
                .build();

            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.kMeans(graph, kmeansConfig);

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "K-Means :: Start",
                    "K-Means :: Initialization :: Start",
                    "K-Means :: Initialization 50%",
                    "K-Means :: Initialization 100%",
                    "K-Means :: Initialization :: Finished",
                    "K-Means :: Main :: Start",
                    "K-Means :: Main :: Iteration 1 of 5 :: Start",
                    "K-Means :: Main :: Iteration 1 of 5 100%",
                    "K-Means :: Main :: Iteration 1 of 5 :: Finished",
                    "K-Means :: Main :: Iteration 2 of 5 :: Start",
                    "K-Means :: Main :: Iteration 2 of 5 100%",
                    "K-Means :: Main :: Iteration 2 of 5 :: Finished",
                    "K-Means :: Main :: Finished",
                    "K-Means :: Finished"
                );
        }

        @Test
        void progressTrackingWithRestarts() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .maxIterations(5)
                .numberOfRestarts(2)
                .k(2)
                .build();

            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.kMeans(graph, kmeansConfig);

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "K-Means :: Start",
                    "K-Means :: KMeans Iteration 1 of 2 :: Start",
                    "K-Means :: KMeans Iteration 1 of 2 :: Initialization :: Start",
                    "K-Means :: KMeans Iteration 1 of 2 :: Initialization 50%",
                    "K-Means :: KMeans Iteration 1 of 2 :: Initialization 100%",
                    "K-Means :: KMeans Iteration 1 of 2 :: Initialization :: Finished",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Start",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Iteration 1 of 5 :: Start",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Iteration 1 of 5 100%",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Iteration 1 of 5 :: Finished",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Iteration 2 of 5 :: Start",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Iteration 2 of 5 100%",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Iteration 2 of 5 :: Finished",
                    "K-Means :: KMeans Iteration 1 of 2 :: Main :: Finished",
                    "K-Means :: KMeans Iteration 1 of 2 :: Finished",
                    "K-Means :: KMeans Iteration 2 of 2 :: Start",
                    "K-Means :: KMeans Iteration 2 of 2 :: Initialization :: Start",
                    "K-Means :: KMeans Iteration 2 of 2 :: Initialization 50%",
                    "K-Means :: KMeans Iteration 2 of 2 :: Initialization 100%",
                    "K-Means :: KMeans Iteration 2 of 2 :: Initialization :: Finished",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Start",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Iteration 1 of 5 :: Start",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Iteration 1 of 5 100%",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Iteration 1 of 5 :: Finished",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Iteration 2 of 5 :: Start",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Iteration 2 of 5 100%",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Iteration 2 of 5 :: Finished",
                    "K-Means :: KMeans Iteration 2 of 2 :: Main :: Finished",
                    "K-Means :: KMeans Iteration 2 of 2 :: Finished",
                    "K-Means :: Finished"
                );
        }

        @Test
        void progressTrackingWithSilhouette() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .maxIterations(5)
                .computeSilhouette(true)
                .numberOfRestarts(1)
                .k(2)
                .build();

            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.kMeans(graph, kmeansConfig);

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "K-Means :: Start",
                    "K-Means :: Initialization :: Start",
                    "K-Means :: Initialization 50%",
                    "K-Means :: Initialization 100%",
                    "K-Means :: Initialization :: Finished",
                    "K-Means :: Main :: Start",
                    "K-Means :: Main :: Iteration 1 of 5 :: Start",
                    "K-Means :: Main :: Iteration 1 of 5 100%",
                    "K-Means :: Main :: Iteration 1 of 5 :: Finished",
                    "K-Means :: Main :: Iteration 2 of 5 :: Start",
                    "K-Means :: Main :: Iteration 2 of 5 100%",
                    "K-Means :: Main :: Iteration 2 of 5 :: Finished",
                    "K-Means :: Main :: Finished",
                    "K-Means :: Silhouette :: Start",
                    "K-Means :: Silhouette 25%",
                    "K-Means :: Silhouette 50%",
                    "K-Means :: Silhouette 75%",
                    "K-Means :: Silhouette 100%",
                    "K-Means :: Silhouette :: Finished",
                    "K-Means :: Finished"
                );
        }
    }
    @Nested
    @GdlExtension
    class LabelPropagation{

        @GdlGraph
        private static final String GRAPH =
            "CREATE" +
                "  (nAlice:User   {seedId: 2})" +
                ", (nBridget:User {seedId: 3})" +
                ", (nCharles:User {seedId: 4})" +
                ", (nDoug:User    {seedId: 3})" +
                ", (nMark:User    {seedId: 4})" +
                ", (nMichael:User {seedId: 2})" +
                ", (nAlice)-[:FOLLOW]->(nBridget)" +
                ", (nAlice)-[:FOLLOW]->(nCharles)" +
                ", (nMark)-[:FOLLOW]->(nDoug)" +
                ", (nBridget)-[:FOLLOW]->(nMichael)" +
                ", (nDoug)-[:FOLLOW]->(nMark)" +
                ", (nMichael)-[:FOLLOW]->(nAlice)" +
                ", (nAlice)-[:FOLLOW]->(nMichael)" +
                ", (nBridget)-[:FOLLOW]->(nAlice)" +
                ", (nMichael)-[:FOLLOW]->(nBridget)" +
                ", (nCharles)-[:FOLLOW]->(nDoug)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldLogProgress() {
            var config = LabelPropagationStreamConfigImpl.builder().build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            var result = algorithms.labelPropagation(graph,config);
            var testTracker = progressTrackerCreator.progressTracker().get();
            List<AtomicLong> progresses = testTracker.getProgresses();

            // Should log progress for every iteration + init step
            assertEquals(result.ranIterations() + 3, progresses.size());
            progresses.forEach(progress -> assertTrue(progress.get() <= graph.relationshipCount()));
            assertTrue(log.containsMessage(TestLog.INFO, ":: Start"));
            LongStream.range(1, result.ranIterations() + 1).forEach(iteration -> {
                assertTrue(log.containsMessage(TestLog.INFO, "Iteration %d of %d :: Start".formatted(iteration, config.maxIterations())));
            });
            assertTrue(log.containsMessage(TestLog.INFO, ":: Finished"));
        }
    }


    abstract static class TestProgressTrackerCreator  extends   ProgressTrackerCreator {

        public TestProgressTrackerCreator(Log log, RequestScopedDependencies requestScopedDependencies) {
            super(log, requestScopedDependencies);
        }

        abstract AtomicReference<TestProgressTracker>  progressTracker();

    }

TestProgressTrackerCreator progressTrackerCreator(int concurrency, Log log) {

    AtomicReference<TestProgressTracker> progressTrackerAtomicReference=new AtomicReference<>();
        var progressTrackerCreator = mock(TestProgressTrackerCreator.class);
        when(progressTrackerCreator.createProgressTracker(any(), any(Task.class))).then(
            i ->
            {
                var taskProgressTracker = new TestProgressTracker(
                    i.getArgument(1),
                    log,
                    new Concurrency(concurrency),
                    EmptyTaskRegistryFactory.INSTANCE
                );
                progressTrackerAtomicReference.set(taskProgressTracker);
                return taskProgressTracker;
            }
        );
    when(progressTrackerCreator.progressTracker()).thenReturn(progressTrackerAtomicReference);

    return progressTrackerCreator;
    }

}


