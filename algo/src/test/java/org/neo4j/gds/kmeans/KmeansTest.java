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
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.assertj.Extractors.replaceTimings;

@GdlExtension
class KmeansTest {
    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a {  kmeans: [1.0, 1.0]} )" +
        "  (b {  kmeans: [1.0, 2.0]} )" +
        "  (c {  kmeans: [102.0, 100.0]} )" +
        "  (d {  kmeans: [100.0, 102.0]} )";
    @Inject
    private Graph graph;

    @GdlGraph(graphNamePrefix = "float")
    private static final String floatQuery =
        "CREATE" +
        "  (a {  kmeans: [1.0f, 1.0f]} )" +
        "  (b {  kmeans: [1.0f, 2.0f]} )" +
        "  (c {  kmeans: [102.0f, 100.0f]} )" +
        "  (d {  kmeans: [100.0f, 102.0f]} )";
    @Inject
    private Graph floatGraph;

    @GdlGraph(graphNamePrefix = "line")
    private static final String LineQuery =
        "CREATE" +
        "  (a {  kmeans: [0.21, 0.0]} )" +
        "  (b {  kmeans: [2.0, 0.0]} )" +
        "  (c {  kmeans: [2.1, 0.0]} )" +
        "  (d {  kmeans: [3.8, 0.0]} )" +
        "  (e {  kmeans: [2.1, 0.0]} )";

    @Inject
    private TestGraph lineGraph;

    @GdlGraph(graphNamePrefix = "nan")
    private static final String nanQuery =
        "CREATE" +
        "  (a {  kmeans: [0.21d, 0.0d]} )" +
        "  (b {  kmeans: [2.0d, NaN]} )" +
        "  (c {  kmeans: [2.1d, 0.0d]} )";

    @Inject
    private TestGraph nanGraph;


    @GdlGraph(graphNamePrefix = "miss")
    private static final String missQuery =
        "CREATE" +
        "  (a {  kmeans: [0.21d, 0.0d]} )" +
        "  (b {  kmeans: [2.0d]} )" +
        "  (c {  kmeans: [2.1d, 0.0d]} )";

    @Inject
    private TestGraph missGraph;

    @Inject
    private IdFunction idFunction;

    @Test
    void shouldThrowOnNan() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(nanGraph, kmeansConfig, kmeansContext);
        assertThatThrownBy(kmeans::compute).hasMessageContaining(
            "Input for K-Means should not contain any NaN values");

    }

    @Test
    void shouldThrowOnDifferentDimensions() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(missGraph, kmeansConfig, kmeansContext);
        assertThatThrownBy(kmeans::compute).hasMessageContaining(
            "All property arrays for K-Means should have the same number of dimensions");

    }

    @Test
    void shouldRun() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
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
    void shouldRunOnFloatGraph() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(floatGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();
        var centers = result.centers();
        assertThat(communities.get(0)).isEqualTo(communities.get(1));
        assertThat(communities.get(2)).isEqualTo(communities.get(3));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(2));

        assertThat(centers[0]).isEqualTo(new double[]{1.0, 1.5});
        assertThat(centers[1]).isEqualTo(new double[]{101, 101});

    }

    @Test
    void shouldWorkOnLineGraphWithOneIteration() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .maxIterations(1)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();
        assertThat(communities.get(0)).isEqualTo(communities.get(1));
        assertThat(communities.get(2)).isEqualTo(communities.get(3)).isEqualTo(communities.get(4));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(2));
    }

    @Test
    void shouldChangeOnLineGraphWithTwoIterations() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L) //init clusters 0.21 and 3.8
            .k(2)
            .maxIterations(2)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        var communities = result.communities();

        assertThat(communities.get(1))
            .isEqualTo(communities.get(2))
            .isEqualTo(communities.get(3))
            .isEqualTo(communities.get(4));
        assertThat(communities.get(0)).isNotEqualTo(communities.get(1));
    }

    @Test
    void shouldComputeSilhouetteCorrectly() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .k(2)
            .computeSilhouette(true)
            .build();
        var kmeansContext = ImmutableKmeansContext.builder().build();

        var kmeans = Kmeans.createKmeans(graph, kmeansConfig, kmeansContext);
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

        assertThat(averageSilhouette).isCloseTo((silhouette.get(0) + silhouette.get(1) + silhouette.get(2) + silhouette.get(
            3)) / 4.0, Offset.offset(1e-4));

    }

    @Test
    void shouldNotWorkForRestartsAndSeeds() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .seedCentroids(List.of(List.of(1d), List.of(2d)))
            .k(2)
            .numberOfRestarts(10)
            .build();

        var kmeansAlgorithmFactory = new KmeansAlgorithmFactory<>();
        assertThatThrownBy(() -> kmeansAlgorithmFactory.build(
            lineGraph,
            kmeansConfig,
            ProgressTracker.NULL_TRACKER
        )).hasMessageContaining("cannot be run");
    }

    @Test
    void shouldNotWorkForDifferentSeedAndK() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .seedCentroids(List.of(List.of(1d)))
            .k(2)
            .build();

        var kmeansAlgorithmFactory = new KmeansAlgorithmFactory<>();
        assertThatThrownBy(() -> kmeansAlgorithmFactory.build(
            lineGraph,
            kmeansConfig,
            ProgressTracker.NULL_TRACKER
        )).hasMessageContaining("Incorrect");
    }

    @Test
    void shouldNotWorkForSeedingWithWrongDimensions() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .seedCentroids(List.of(List.of(1d, 2d)))
            .k(1)
            .build();

        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(missGraph, kmeansConfig, kmeansContext);
        assertThatThrownBy(kmeans::compute).hasMessageContaining("same");
    }

    @Test
    void shouldNotWorkForSeedingWithNaN() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .seedCentroids(List.of(List.of(Double.NaN, 2.0)))
            .k(1)
            .build();

        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(missGraph, kmeansConfig, kmeansContext);
        assertThatThrownBy(kmeans::compute).hasMessageContaining("NaN");
    }

    @Test
    void shouldWithSeededCentroids() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .seedCentroids(List.of(List.of(5.0, 0.0), List.of(100.0d, 0.0d)))
            .k(2)
            .build();

        var kmeansContext = ImmutableKmeansContext.builder().build();
        var kmeans = Kmeans.createKmeans(lineGraph, kmeansConfig, kmeansContext);
        var result = kmeans.compute();
        assertThat(result.communities().toArray()).isEqualTo(new int[]{0, 0, 0, 0, 0});
        var secondCentroid = result.centers()[1];
        assertThat(secondCentroid).isEqualTo(new double[]{100.0d, 0.0d});
    }

    @Test
    void progressTracking() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .maxIterations(5)
            .numberOfRestarts(1)
            .k(2)
            .build();

        var factory = new KmeansAlgorithmFactory<KmeansBaseConfig>();
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            factory.progressTask(graph, kmeansConfig),
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );

        factory.build(graph, kmeansConfig, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Kmeans :: Start",
                "Kmeans :: Initialization :: Start",
                "Kmeans :: Initialization 50%",
                "Kmeans :: Initialization 100%",
                "Kmeans :: Initialization :: Finished",
                "Kmeans :: Main :: Start",
                "Kmeans :: Main :: Iteration 1 of 5 :: Start",
                "Kmeans :: Main :: Iteration 1 of 5 100%",
                "Kmeans :: Main :: Iteration 1 of 5 :: Finished",
                "Kmeans :: Main :: Iteration 2 of 5 :: Start",
                "Kmeans :: Main :: Iteration 2 of 5 100%",
                "Kmeans :: Main :: Iteration 2 of 5 :: Finished",
                "Kmeans :: Main :: Finished",
                "Kmeans :: Finished"
            );
    }

    @Test
    void progressTrackingWithRestarts() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .maxIterations(5)
            .numberOfRestarts(2)
            .k(2)
            .build();

        var factory = new KmeansAlgorithmFactory<KmeansBaseConfig>();
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            factory.progressTask(graph, kmeansConfig),
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );

        factory.build(graph, kmeansConfig, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Kmeans :: Start",
                "Kmeans :: KMeans Iteration 1 of 2 :: Start",
                "Kmeans :: KMeans Iteration 1 of 2 :: Initialization :: Start",
                "Kmeans :: KMeans Iteration 1 of 2 :: Initialization 50%",
                "Kmeans :: KMeans Iteration 1 of 2 :: Initialization 100%",
                "Kmeans :: KMeans Iteration 1 of 2 :: Initialization :: Finished",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Start",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Iteration 1 of 5 :: Start",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Iteration 1 of 5 100%",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Iteration 1 of 5 :: Finished",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Iteration 2 of 5 :: Start",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Iteration 2 of 5 100%",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Iteration 2 of 5 :: Finished",
                "Kmeans :: KMeans Iteration 1 of 2 :: Main :: Finished",
                "Kmeans :: KMeans Iteration 1 of 2 :: Finished",
                "Kmeans :: KMeans Iteration 2 of 2 :: Start",
                "Kmeans :: KMeans Iteration 2 of 2 :: Initialization :: Start",
                "Kmeans :: KMeans Iteration 2 of 2 :: Initialization 50%",
                "Kmeans :: KMeans Iteration 2 of 2 :: Initialization 100%",
                "Kmeans :: KMeans Iteration 2 of 2 :: Initialization :: Finished",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Start",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Iteration 1 of 5 :: Start",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Iteration 1 of 5 100%",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Iteration 1 of 5 :: Finished",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Iteration 2 of 5 :: Start",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Iteration 2 of 5 100%",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Iteration 2 of 5 :: Finished",
                "Kmeans :: KMeans Iteration 2 of 2 :: Main :: Finished",
                "Kmeans :: KMeans Iteration 2 of 2 :: Finished",
                "Kmeans :: Finished"
            );
    }

    @Test
    void progressTrackingWithSilhouette() {
        var kmeansConfig = ImmutableKmeansStreamConfig.builder()
            .nodeProperty("kmeans")
            .concurrency(1)
            .randomSeed(19L)
            .maxIterations(5)
            .computeSilhouette(true)
            .numberOfRestarts(1)
            .k(2)
            .build();

        var factory = new KmeansAlgorithmFactory<KmeansBaseConfig>();
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(
            factory.progressTask(graph, kmeansConfig),
            log,
            4,
            EmptyTaskRegistryFactory.INSTANCE
        );

        factory.build(graph, kmeansConfig, progressTracker).compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .extracting(replaceTimings())
            .containsExactly(
                "Kmeans :: Start",
                "Kmeans :: Initialization :: Start",
                "Kmeans :: Initialization 50%",
                "Kmeans :: Initialization 100%",
                "Kmeans :: Initialization :: Finished",
                "Kmeans :: Main :: Start",
                "Kmeans :: Main :: Iteration 1 of 5 :: Start",
                "Kmeans :: Main :: Iteration 1 of 5 100%",
                "Kmeans :: Main :: Iteration 1 of 5 :: Finished",
                "Kmeans :: Main :: Iteration 2 of 5 :: Start",
                "Kmeans :: Main :: Iteration 2 of 5 100%",
                "Kmeans :: Main :: Iteration 2 of 5 :: Finished",
                "Kmeans :: Main :: Finished",
                "Kmeans :: Silhouette :: Start",
                "Kmeans :: Silhouette 25%",
                "Kmeans :: Silhouette 50%",
                "Kmeans :: Silhouette 75%",
                "Kmeans :: Silhouette 100%",
                "Kmeans :: Silhouette :: Finished",
                "Kmeans :: Finished"
            );
    }

}
