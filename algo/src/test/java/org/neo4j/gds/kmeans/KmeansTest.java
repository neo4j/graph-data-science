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
package org.neo4j.gds.kmeans;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.CommunityAlgorithmTasks;
import org.neo4j.gds.TestProgressTrackerHelper;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class KmeansTest {

    @GdlExtension
    @Nested
    class KmeansDoubleTest{
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
        void shouldRun() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .build();
            var kmeansContext = KmeansContext.defaults();

            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();
            var communities = result.communities();
            var distances = result.distanceFromCenter();
            var centers = result.centers();

            var averageDistance = result.averageDistanceToCentroid();


            assertThat(communities.get(0)).isEqualTo(communities.get(1));
            assertThat(communities.get(2)).isEqualTo(communities.get(3));
            assertThat(communities.get(0)).isNotEqualTo(communities.get(2));

            assertThat(distances.get(0)).isEqualTo(0.5);  //centre is (1,1.5)  sqrt ( 0^2 + 0.5^2)
            assertThat(distances.get(1)).isEqualTo(0.5);
            assertThat(distances.get(2)).isCloseTo(Math.sqrt(2), Offset.offset(1e-4)); //centre is (101,101) sqrt (1^2+1^2)
            assertThat(distances.get(3)).isCloseTo(Math.sqrt(2), Offset.offset(1e-4));

            assertThat(centers[0]).isEqualTo(new double[]{1.0, 1.5});
            assertThat(centers[1]).isEqualTo(new double[]{101, 101});

            //distances are  anyway checked above, so we just take their mean as one last confirmation
            assertThat(averageDistance).isCloseTo(distances.stream().sum() / 4.0, Offset.offset(1e-4));

        }

        @Test
        void shouldComputeSilhouetteCorrectly() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .computeSilhouette(true)
                .build();
            var kmeansContext = KmeansContext.defaults();

            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();

            var silhouette = result.silhouette();
            var averageSilhouette = result.averageSilhouette();

            // d(a,b) =1   d(a,c)= sqrt(101^2 + 99^2)   d(a,d)=sqrt(101^2 + 99^2)
            // s(a) = 1 -1/sqrt(101^2+99^2)
            assertThat(silhouette.get(0)).isCloseTo(0.99292928571, Offset.offset(1e-4));

            //d(b,c) =  sqrt(101^2 + 98^2)  d(b,d) = sqrt(99^2 + 100^2)
            // s(b) = 1 - 2/(sqrt(101^2 + 98^2) + sqrt(99^2 + 100^2))
            assertThat(silhouette.get(1)).isCloseTo(0.99289384777, Offset.offset(1e-4));


            //d(c,d) = sqrt(8)
            //s(c) = 1 - 2*sqrt(8) / (sqrt(101^2 + 99^2) + sqrt(101^2 + 98^2) )
            assertThat(silhouette.get(2)).isCloseTo(0.97995151331, Offset.offset(1e-4));

            //s(d) = 1 - 2*sqrt(8) / (sqrt(101^2 + 99^2) + sqrt(99^2 + 100^2) )
            assertThat(silhouette.get(3)).isCloseTo(0.97995050342, Offset.offset(1e-4));

            assertThat(averageSilhouette).isCloseTo(
                (silhouette.get(0) + silhouette.get(1) + silhouette.get(2) + silhouette.get(
                    3)) / 4.0, Offset.offset(1e-4)
            );

        }

        @ParameterizedTest
        @ValueSource(strings = {"fail", "kfail"})
        void shouldThrowForMissingProperty(String property) {

            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty(property)
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .build();
            var kmeansContext = KmeansContext.defaults();

            assertThatThrownBy(
                () -> Kmeans.createKmeans(
                    graph,
                    kmeansConfig.toParameters(),
                    kmeansContext,
                    TerminationFlag.RUNNING_TRUE
                ).compute()
            ).hasMessageContaining(property);
        }


    }



    @GdlExtension
    @Nested
    class KmeansFloatTest {
        @GdlGraph
        private static final String floatQuery =
            "CREATE" +
                "  (a {  kmeans: [1.0f, 1.0f]} )" +
                "  (b {  kmeans: [1.0f, 2.0f]} )" +
                "  (c {  kmeans: [102.0f, 100.0f]} )" +
                "  (d {  kmeans: [100.0f, 102.0f]} )";
        @Inject
        private Graph graph;

        @Test
        void shouldRunOnFloatGraph() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .build();
            var kmeansContext = KmeansContext.defaults();

            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();
            var communities = result.communities();
            var centers = result.centers();
            assertThat(communities.get(0)).isEqualTo(communities.get(1));
            assertThat(communities.get(2)).isEqualTo(communities.get(3));
            assertThat(communities.get(0)).isNotEqualTo(communities.get(2));

            assertThat(centers[0]).isEqualTo(new double[]{1.0, 1.5});
            assertThat(centers[1]).isEqualTo(new double[]{101, 101});

        }

    }

    @GdlExtension
    @Nested
    class KmeansLineGraph {

        @GdlGraph
        private static final String LineQuery =
            "CREATE" +
                "  (a {  kmeans: [0.21, 0.0], kmeans1d: 0.21} )" +
                "  (b {  kmeans: [2.0, 0.0],  kmeans1d: 2.0} )" +
                "  (c {  kmeans: [2.1, 0.0], kmeans1d: 2.1 } )" +
                "  (d {  kmeans: [3.8, 0.0], kmeans1d: 3.8} )" +
                "  (e {  kmeans: [2.1, 0.0], kmeans1d: 2.1} )";

        @Inject
        private TestGraph graph;


        @ParameterizedTest
        @CsvSource({"kmeans", "kmeans1d"})
        void shouldWorkOnLineGraphWithOneIteration(String property) {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty(property)
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .maxIterations(1)
                .build();
            var kmeansContext = KmeansContext.defaults();

            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();
            var communities = result.communities();
            assertThat(communities.get(0)).isEqualTo(communities.get(1));
            assertThat(communities.get(2)).isEqualTo(communities.get(3)).isEqualTo(communities.get(4));
            assertThat(communities.get(0)).isNotEqualTo(communities.get(2));
        }

        @ParameterizedTest
        @CsvSource({"kmeans", "kmeans1d"})
        void shouldChangeOnLineGraphWithTwoIterations(String property) {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty(property)
                .concurrency(1)
                .randomSeed(19L) //init clusters 0.21 and 3.8
                .k(2)
                .maxIterations(2)
                .build();
            var kmeansContext = KmeansContext.defaults();

            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();
            var communities = result.communities();

            assertThat(communities.get(1))
                .isEqualTo(communities.get(2))
                .isEqualTo(communities.get(3))
                .isEqualTo(communities.get(4));
            assertThat(communities.get(0)).isNotEqualTo(communities.get(1));
        }

        @Test
        void shouldWithSeededCentroids() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .seedCentroids(List.of(List.of(5.0, 0.0), List.of(100.0d, 0.0d)))
                .k(2)
                .build();

            var kmeansContext = KmeansContext.defaults();
            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();
            assertThat(result.communities().toArray()).isEqualTo(new int[]{0, 0, 0, 0, 0});
            var secondCentroid = result.centers()[1];
            assertThat(secondCentroid).isEqualTo(new double[]{100.0d, 0.0d});
        }

        @Test
        void shouldWithSeededCentroidsOnScalars() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans1d")
                .concurrency(1)
                .randomSeed(19L)
                .seedCentroids(List.of(List.of(5.0), List.of(100.0d)))
                .k(2)
                .build();

            var kmeansContext = KmeansContext.defaults();
            var kmeans = Kmeans.createKmeans(
                graph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            var result = kmeans.compute();
            assertThat(result.communities().toArray()).isEqualTo(new int[]{0, 0, 0, 0, 0});
            var secondCentroid = result.centers()[1];
            assertThat(secondCentroid).isEqualTo(new double[]{100.0d});
        }


    }

    @GdlExtension
    @Nested
    class KmeansValidationsTest {
        @GdlGraph(graphNamePrefix = "miss")
        private static final String missQuery =
            "CREATE" +
                "  (a {  kmeans: [0.21d, 0.0d]} )" +
                "  (b {  kmeans: [2.0d]} )" +
                "  (c {  kmeans: [2.1d, 0.0d]} )";

        @GdlGraph(graphNamePrefix = "nan")
        private static final String nanQuery =
            "CREATE" +
                "  (a {  kmeans: [0.21d, 0.0d]} )" +
                "  (b {  kmeans: [2.0d, NaN]} )" +
                "  (c {  kmeans: [2.1d, 0.0d]} )";

        @Inject
        private TestGraph nanGraph;

        @Inject
        private TestGraph missGraph;

        @Test
        void shouldThrowOnNan() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .build();
            var kmeansContext = KmeansContext.defaults();
            var kmeans = Kmeans.createKmeans(
                nanGraph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            assertThatThrownBy(kmeans::compute).hasMessageContaining(
                "Input for K-Means should not contain any NaN values");

        }

        @Test
        void shouldThrowOnDifferentDimensions() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .k(2)
                .build();
            var kmeansContext = KmeansContext.defaults();
            var kmeans = Kmeans.createKmeans(
                missGraph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            assertThatThrownBy(kmeans::compute).hasMessageContaining(
                "All property arrays for K-Means should have the same number of dimensions");

        }


        @Test
        void shouldNotWorkForRestartsAndSeeds() {
            assertThatThrownBy(
                () -> KmeansStreamConfigImpl.builder()
                    .nodeProperty("kmeans")
                    .concurrency(1)
                    .randomSeed(19L)
                    .seedCentroids(List.of(List.of(1d), List.of(2d)))
                    .k(2)
                    .numberOfRestarts(10)
                    .build()
            )
                .hasMessageContaining("cannot be run");
        }

        @Test
        void shouldNotWorkForDifferentSeedAndK() {
            assertThatThrownBy(() ->
                KmeansStreamConfigImpl.builder()
                    .nodeProperty("kmeans")
                    .concurrency(1)
                    .randomSeed(19L)
                    .seedCentroids(List.of(List.of(1d)))
                    .k(2)
                    .build()
            )
                .hasMessageContaining("Incorrect");
        }

        @Test
        void shouldNotWorkForSeedingWithWrongDimensions() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .seedCentroids(List.of(List.of(1d, 2d)))
                .k(1)
                .build();

            var kmeansContext = KmeansContext.defaults();
            var kmeans = Kmeans.createKmeans(
                missGraph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            assertThatThrownBy(kmeans::compute).hasMessageContaining("same");
        }

        @Test
        void shouldNotWorkForSeedingWithNaN() {
            var kmeansConfig = KmeansStreamConfigImpl.builder()
                .nodeProperty("kmeans")
                .concurrency(1)
                .randomSeed(19L)
                .seedCentroids(List.of(List.of(Double.NaN, 2.0)))
                .k(1)
                .build();

            var kmeansContext = KmeansContext.defaults();
            var kmeans = Kmeans.createKmeans(
                missGraph,
                kmeansConfig.toParameters(),
                kmeansContext,
                TerminationFlag.RUNNING_TRUE
            );
            assertThatThrownBy(kmeans::compute).hasMessageContaining("NaN");
        }
    }

    @Nested
    @GdlExtension
    class ProgressTrackingTest {

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

            var concurrency = new Concurrency(1);

            var parameters = new KmeansParameters(
                2,
                5,
                0.05,
                1,
                false,
                new Concurrency(1),
                "kmeans",
                SamplerType.UNIFORM,
                List.of(),
                Optional.of(19L)
            );

            var progressTrackerWithLog = TestProgressTrackerHelper.create(
                new CommunityAlgorithmTasks().kMeans(
                    graph,
                    parameters
                ), concurrency
            );

            var progressTracker = progressTrackerWithLog.progressTracker();
            var kmeans = Kmeans.createKmeans(
                graph,
                parameters,
                KmeansContext.progressTrackerWithDefaultExecutor(progressTracker),
                TerminationFlag.RUNNING_TRUE
            );

            kmeans.compute();

            var log = progressTrackerWithLog.log();
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
            var concurrency = new Concurrency(1);

            var parameters = new KmeansParameters(
                2,
                5,
                0.05,
                2,
                false,
                new Concurrency(1),
                "kmeans",
                SamplerType.UNIFORM,
                List.of(),
                Optional.of(19L)
            );

            var progressTrackerWithLog = TestProgressTrackerHelper.create(
                new CommunityAlgorithmTasks().kMeans(
                    graph,
                    parameters
                ), concurrency
            );

            var progressTracker = progressTrackerWithLog.progressTracker();
            var kmeans = Kmeans.createKmeans(
                graph,
                parameters,
                KmeansContext.progressTrackerWithDefaultExecutor(progressTracker),
                TerminationFlag.RUNNING_TRUE
            );

            kmeans.compute();

            var log = progressTrackerWithLog.log();
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
            var concurrency = new Concurrency(1);
            var parameters = new KmeansParameters(
                2,
                5,
                0.05,
                1,
                true,
                new Concurrency(1),
                "kmeans",
                SamplerType.UNIFORM,
                List.of(),
                Optional.of(19L)
            );

            var progressTrackerWithLog = TestProgressTrackerHelper.create(
                new CommunityAlgorithmTasks().kMeans(
                    graph,
                    parameters
                ), concurrency
            );

            var progressTracker = progressTrackerWithLog.progressTracker();

            var kmeans = Kmeans.createKmeans(
                graph,
                parameters,
                KmeansContext.progressTrackerWithDefaultExecutor(progressTracker),
                TerminationFlag.RUNNING_TRUE
            );

            kmeans.compute();

            var log = progressTrackerWithLog.log();
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

}
