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
import org.assertj.core.api.AssertionsForInterfaceTypes;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.assertj.Extractors;
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
import org.neo4j.gds.leiden.LeidenStatsConfigImpl;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.louvain.LouvainStreamConfigImpl;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfigImpl;
import org.neo4j.gds.scc.SccStreamConfigImpl;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfigImpl;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfigImpl;
import org.neo4j.gds.wcc.WccStreamConfigImpl;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;
import static org.neo4j.gds.compat.TestLog.INFO;
import static org.neo4j.gds.modularityoptimization.ModularityOptimization.K1COLORING_MAX_ITERATIONS;

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
    @Nested
    @GdlExtension
    class Leiden{

        @GdlGraph(orientation = Orientation.UNDIRECTED)
        private static final String DB_CYPHER =
            "CREATE " +
                "  (a0:Node {optimal: 5000, seed: 1, seed2:-1})," +
                "  (a1:Node {optimal: 4000,seed: 2})," +
                "  (a2:Node {optimal: 5000,seed: 2})," +
                "  (a3:Node {optimal: 5000})," +
                "  (a4:Node {optimal: 5000,seed: 5})," +
                "  (a5:Node {optimal: 4000,seed: 6})," +
                "  (a6:Node {optimal: 4000,seed: 7})," +
                "  (a7:Node {optimal: 4000,seed: 8})," +
                "  (a0)-[:R {weight: 1.0}]->(a1)," +
                "  (a0)-[:R {weight: 1.0}]->(a2)," +
                "  (a0)-[:R {weight: 1.0}]->(a3)," +
                "  (a0)-[:R {weight: 1.0}]->(a4)," +
                "  (a2)-[:R {weight: 1.0}]->(a3)," +
                "  (a2)-[:R {weight: 1.0}]->(a4)," +
                "  (a3)-[:R {weight: 1.0}]->(a4)," +
                "  (a1)-[:R {weight: 1.0}]->(a5)," +
                "  (a1)-[:R {weight: 1.0}]->(a6)," +
                "  (a1)-[:R {weight: 1.0}]->(a7)," +
                "  (a5)-[:R {weight: 1.0}]->(a6)," +
                "  (a5)-[:R {weight: 1.0}]->(a7)," +
                "  (a6)-[:R {weight: 1.0}]->(a7)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldLogProgress() {
            var config = LeidenStatsConfigImpl.builder().maxLevels(3).randomSeed(19L).build();
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
             algorithms.leiden(graph,config);

            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "Leiden :: Start",
                    "Leiden :: Initialization :: Start",
                    "Leiden :: Initialization 12%",
                    "Leiden :: Initialization 25%",
                    "Leiden :: Initialization 37%",
                    "Leiden :: Initialization 50%",
                    "Leiden :: Initialization 62%",
                    "Leiden :: Initialization 75%",
                    "Leiden :: Initialization 87%",
                    "Leiden :: Initialization 100%",
                    "Leiden :: Initialization :: Finished",
                    "Leiden :: Iteration :: Start",
                    "Leiden :: Iteration :: Local Move 1 of 3 :: Start",
                    "Leiden :: Iteration :: Local Move 1 of 3 100%",
                    "Leiden :: Iteration :: Local Move 1 of 3 :: Finished",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 :: Start",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 12%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 25%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 37%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 50%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 62%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 75%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 87%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 100%",
                    "Leiden :: Iteration :: Modularity Computation 1 of 3 :: Finished",
                    "Leiden :: Iteration :: Refinement 1 of 3 :: Start",
                    "Leiden :: Iteration :: Refinement 1 of 3 12%",
                    "Leiden :: Iteration :: Refinement 1 of 3 25%",
                    "Leiden :: Iteration :: Refinement 1 of 3 37%",
                    "Leiden :: Iteration :: Refinement 1 of 3 50%",
                    "Leiden :: Iteration :: Refinement 1 of 3 62%",
                    "Leiden :: Iteration :: Refinement 1 of 3 75%",
                    "Leiden :: Iteration :: Refinement 1 of 3 87%",
                    "Leiden :: Iteration :: Refinement 1 of 3 100%",
                    "Leiden :: Iteration :: Refinement 1 of 3 :: Finished",
                    "Leiden :: Iteration :: Aggregation 1 of 3 :: Start",
                    "Leiden :: Iteration :: Aggregation 1 of 3 12%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 25%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 37%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 50%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 62%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 75%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 87%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 100%",
                    "Leiden :: Iteration :: Aggregation 1 of 3 :: Finished",
                    "Leiden :: Iteration :: Local Move 2 of 3 :: Start",
                    "Leiden :: Iteration :: Local Move 2 of 3 100%",
                    "Leiden :: Iteration :: Local Move 2 of 3 :: Finished",
                    "Leiden :: Iteration :: Modularity Computation 2 of 3 :: Start",
                    "Leiden :: Iteration :: Modularity Computation 2 of 3 100%",
                    "Leiden :: Iteration :: Modularity Computation 2 of 3 :: Finished",
                    "Leiden :: Iteration :: Finished",
                    "Leiden :: Finished"
                );
        }
    }
    @Nested
    class LocalClustering {

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void progressLogging(boolean useSeed) {
            var graph = TestSupport.fromGdl("CREATE" +
                " (a {triangles: 2})" +
                " (b {triangles: 2})" +
                " (c {triangles: 1})" +
                " (d {triangles: 1})" +
                " (a)-[:T]->(b)-[:T]->(c)-[:T]->(a)" +
                ",(a)-[:T]->(b)" +
                ",(d)-[:T]->(a)", Orientation.UNDIRECTED).graph();

            var configbuilder = LocalClusteringCoefficientBaseConfigImpl.builder();
            if (useSeed) {
                configbuilder = configbuilder.seedProperty("triangles");
            }
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(4,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.lcc(graph,configbuilder.build());

            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Start");
            if (!useSeed) {
                log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: IntersectingTriangleCount :: Start");
                log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: IntersectingTriangleCount 25%");
                log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: IntersectingTriangleCount 50%");
                log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: IntersectingTriangleCount 75%");
                log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: IntersectingTriangleCount 100%");
                log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: IntersectingTriangleCount :: Finished");
            }
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Calculate Local Clustering Coefficient :: Start");
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Calculate Local Clustering Coefficient 25%");
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Calculate Local Clustering Coefficient 50%");
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Calculate Local Clustering Coefficient 75%");
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Calculate Local Clustering Coefficient 100%");
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Calculate Local Clustering Coefficient :: Finished");
            log.assertContainsMessage(TestLog.INFO, "LocalClusteringCoefficient :: Finished");
        }
    }
    @Nested
    @GdlExtension
    class Louvain{

        @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 0)
        private static final String DB_CYPHER =
            "CREATE" +
                "  (a:Node {seed: 1,seed2: -1})" +        // 0
                ", (b:Node {seed: 1,seed2: 10})" +        // 1
                ", (c:Node {seed: 1})" +        // 2
                ", (d:Node {seed: 1})" +        // 3
                ", (e:Node {seed: 1})" +        // 4
                ", (f:Node {seed: 1})" +        // 5
                ", (g:Node {seed: 2})" +        // 6
                ", (h:Node {seed: 2})" +        // 7
                ", (i:Node {seed: 2})" +        // 8
                ", (j:Node {seed: 42})" +       // 9
                ", (k:Node {seed: 42})" +       // 10
                ", (l:Node {seed: 42})" +       // 11
                ", (m:Node {seed: 42})" +       // 12
                ", (n:Node {seed: 42})" +       // 13
                ", (x:Node {seed: 1})" +        // 14
                ", (u:Some)" +
                ", (v:Other)" +
                ", (w:Label)" +

                ", (a)-[:TYPE_OUT {weight: 1.0}]->(b)" +
                ", (a)-[:TYPE_OUT {weight: 1.0}]->(d)" +
                ", (a)-[:TYPE_OUT {weight: 1.0}]->(f)" +
                ", (b)-[:TYPE_OUT {weight: 1.0}]->(d)" +
                ", (b)-[:TYPE_OUT {weight: 1.0}]->(x)" +
                ", (b)-[:TYPE_OUT {weight: 1.0}]->(g)" +
                ", (b)-[:TYPE_OUT {weight: 1.0}]->(e)" +
                ", (c)-[:TYPE_OUT {weight: 1.0}]->(x)" +
                ", (c)-[:TYPE_OUT {weight: 1.0}]->(f)" +
                ", (d)-[:TYPE_OUT {weight: 1.0}]->(k)" +
                ", (e)-[:TYPE_OUT {weight: 1.0}]->(x)" +
                ", (e)-[:TYPE_OUT {weight: 0.01}]->(f)" +
                ", (e)-[:TYPE_OUT {weight: 1.0}]->(h)" +
                ", (f)-[:TYPE_OUT {weight: 1.0}]->(g)" +
                ", (g)-[:TYPE_OUT {weight: 1.0}]->(h)" +
                ", (h)-[:TYPE_OUT {weight: 1.0}]->(i)" +
                ", (h)-[:TYPE_OUT {weight: 1.0}]->(j)" +
                ", (i)-[:TYPE_OUT {weight: 1.0}]->(k)" +
                ", (j)-[:TYPE_OUT {weight: 1.0}]->(k)" +
                ", (j)-[:TYPE_OUT {weight: 1.0}]->(m)" +
                ", (j)-[:TYPE_OUT {weight: 1.0}]->(n)" +
                ", (k)-[:TYPE_OUT {weight: 1.0}]->(m)" +
                ", (k)-[:TYPE_OUT {weight: 1.0}]->(l)" +
                ", (l)-[:TYPE_OUT {weight: 1.0}]->(n)" +
                ", (m)-[:TYPE_OUT {weight: 1.0}]->(n)";

        @Inject
        private GraphStore graphStore;

        @Test
        void testLogging() {
            var graph = graphStore.getGraph(
                NodeLabel.listOf("Node"),
                RelationshipType.listOf("TYPE_OUT"),
                Optional.empty()
            );
            var concurrency = new Concurrency(4);
            var maxIterations = 10;
            var maxLevels = 10;
            var log = new GdsTestLog();
            var progressTrackerCreator = progressTrackerCreator(4,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            var config= LouvainStreamConfigImpl.builder().concurrency(concurrency).maxIterations(maxIterations).maxLevels(maxLevels).build();
            algorithms.louvain(graph,config);

            assertTrue(log.containsMessage(INFO, ":: Start"));
            assertTrue(log.containsMessage(INFO, ":: Finished"));
        }
    }
    @GdlExtension
    @Nested
    class ModularityOptimization{

        @GdlGraph(orientation = Orientation.UNDIRECTED, idOffset = 0)
        private static final String DB_CYPHER =
            "CREATE" +
                "  (a:Node {seed1:  1,  seed2: 21})" +
                ", (b:Node {seed1: 5})" +
                ", (c:Node {seed1:  2,  seed2: 42})" +
                ", (d:Node {seed1:  3,  seed2: 33})" +
                ", (e:Node {seed1:  2,  seed2: 42})" +
                ", (f:Node {seed1:  3,  seed2: 33})" +

                ", (a)-[:TYPE_OUT {weight: 0.01}]->(b)" +
                ", (a)-[:TYPE_OUT {weight: 5.0}]->(e)" +
                ", (a)-[:TYPE_OUT {weight: 5.0}]->(f)" +
                ", (b)-[:TYPE_OUT {weight: 5.0}]->(c)" +
                ", (b)-[:TYPE_OUT {weight: 5.0}]->(d)" +
                ", (c)-[:TYPE_OUT {weight: 0.01}]->(e)" +
                ", (f)-[:TYPE_OUT {weight: 0.01}]->(d)";

        @Inject
        private TestGraph graph;

        @Test
        void testLogging() {
            var log = new GdsTestLog();
            var config = ModularityOptimizationStreamConfigImpl.builder().maxIterations(K1COLORING_MAX_ITERATIONS).concurrency(new Concurrency(3)).batchSize(2).build();

            var progressTrackerCreator = progressTrackerCreator(2,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.modularityOptimization(graph,config);
            assertThat(log.getMessages(INFO))
                .extracting(Extractors.removingThreadId())
                .contains(
                    "ModularityOptimization :: Start",
                    "ModularityOptimization :: initialization :: K1Coloring :: color nodes 1 of 5 :: Start",
                    "ModularityOptimization :: initialization :: K1Coloring :: color nodes 1 of 5 :: Finished",
                    "ModularityOptimization :: initialization :: K1Coloring :: validate nodes 1 of 5 :: Start",
                    "ModularityOptimization :: initialization :: K1Coloring :: validate nodes 1 of 5 :: Finished",
                    "ModularityOptimization :: compute modularity :: optimizeForColor 1 of 5 :: Start",
                    "ModularityOptimization :: compute modularity :: optimizeForColor 1 of 5 :: Finished",
                    "ModularityOptimization :: Finished"
                );
        }

    }
    @Nested
    @GdlExtension
    class Scc{

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
                "  (a:Node)" +
                ", (b:Node)" +
                ", (c:Node)" +
                ", (d:Node)" +
                ", (e:Node)" +
                ", (f:Node)" +
                ", (g:Node)" +
                ", (h:Node)" +
                ", (i:Node)" +

                ", (a)-[:TYPE {cost: 5}]->(b)" +
                ", (b)-[:TYPE {cost: 5}]->(c)" +
                ", (c)-[:TYPE {cost: 5}]->(a)" +

                ", (d)-[:TYPE {cost: 2}]->(e)" +
                ", (e)-[:TYPE {cost: 2}]->(f)" +
                ", (f)-[:TYPE {cost: 2}]->(d)" +

                ", (a)-[:TYPE {cost: 2}]->(d)" +

                ", (g)-[:TYPE {cost: 3}]->(h)" +
                ", (h)-[:TYPE {cost: 3}]->(i)" +
                ", (i)-[:TYPE {cost: 3}]->(g)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldLogProgress() {
            var config = SccStreamConfigImpl.builder().build();
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.scc(graph,config);
            assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "SCC :: Start",
                    "SCC 11%",
                    "SCC 22%",
                    "SCC 33%",
                    "SCC 44%",
                    "SCC 55%",
                    "SCC 66%",
                    "SCC 77%",
                    "SCC 88%",
                    "SCC 100%",
                    "SCC :: Finished"
                );
        }
    }

    @Nested
    class Wcc{

        private static final int SETS_COUNT = 16;
        private static final int SET_SIZE = 10;

        @ParameterizedTest
        @EnumSource(Orientation.class)
        void shouldLogProgress(Orientation orientation) {
            var graph = createTestGraph(orientation);

            var config = WccStreamConfigImpl.builder().concurrency(2).build();
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(2,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.wcc(graph,config);

            var messagesInOrder = log.getMessages(INFO);

            AssertionsForInterfaceTypes.assertThat(messagesInOrder)
                // avoid asserting on the thread id
                .extracting(removingThreadId())
                .hasSize(103)
                .containsSequence(
                    "WCC :: Start",
                    "WCC 0%",
                    "WCC 1%",
                    "WCC 2%"
                )
                .containsSequence(
                    "WCC 98%",
                    "WCC 99%",
                    "WCC 100%",
                    "WCC :: Finished"
                );
        }

        private static Graph createTestGraph(Orientation orientation) {
            int[] setSizes = new int[SETS_COUNT];
            Arrays.fill(setSizes, SET_SIZE);

            StringBuilder gdl = new StringBuilder();

            for (int setSize : setSizes) {
                gdl.append(createLine(setSize));
            }

            return fromGdl(gdl.toString(), orientation);
        }

        static String createLine(int setSize) {
            return IntStream.range(0, setSize)
                .mapToObj(i -> "()")
                .collect(Collectors.joining("-[:REL]->"));
        }


    }

    @Nested
    @GdlExtension
    class SpeakerListenerLPA{

        // using an offset of 0 as the tests were written against a specific random seed
        @GdlGraph(idOffset = 0)
        private static final String GDL =
            "(x), (a), (b), (c), (d), (e), (f), (g), (h), (i)" +
                ", (a)-->(b)" +
                ", (a)-->(c)" +
                ", (b)-->(e)" +
                ", (b)-->(d)" +
                ", (b)-->(c)" +
                ", (e)-->(f)" +
                ", (f)-->(g)" +
                ", (f)-->(h)" +
                ", (f)-->(i)" +
                ", (h)-->(i)" +
                ", (g)-->(i)";

        @Inject
        private TestGraph graph;

        @Test
        void shouldLogProgress(){

            var config = SpeakerListenerLPAConfigImpl.builder().concurrency(1).maxIterations(5).build();
            var log = new GdsTestLog();

            var progressTrackerCreator = progressTrackerCreator(1,log);

            var algorithms = new CommunityAlgorithms(progressTrackerCreator, TerminationFlag.RUNNING_TRUE);
            algorithms.speakerListenerLPA(graph,config);
            Assertions.assertThat(log.getMessages(TestLog.INFO))
                .extracting(removingThreadId())
                .extracting(replaceTimings())
                .containsExactly(
                    "SpeakerListenerLPA :: Start",
                    "SpeakerListenerLPA :: Compute iteration 1 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 1 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 1 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 1 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 1 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 1 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 2 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 2 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 2 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 2 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 2 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 2 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 3 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 3 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 3 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 3 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 3 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 3 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 4 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 4 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 4 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 4 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 4 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 4 of 5 :: Finished",
                    "SpeakerListenerLPA :: Compute iteration 5 of 5 :: Start",
                    "SpeakerListenerLPA :: Compute iteration 5 of 5 100%",
                    "SpeakerListenerLPA :: Compute iteration 5 of 5 :: Finished",
                    "SpeakerListenerLPA :: Master compute iteration 5 of 5 :: Start",
                    "SpeakerListenerLPA :: Master compute iteration 5 of 5 100%",
                    "SpeakerListenerLPA :: Master compute iteration 5 of 5 :: Finished",
                    "SpeakerListenerLPA :: Finished"
                );
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


