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
package org.neo4j.graphalgo.pagerank;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.scaling.ScalarScaler;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;
import org.neo4j.graphalgo.extension.TestGraph;
import org.neo4j.graphalgo.pagerank.PageRankPregelAlgorithmFactory.Mode;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;
import static org.neo4j.graphalgo.TestSupport.assertMemoryEstimation;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class PageRankPregelTest {

    private static final double SCORE_PRECISION = 1E-5;

    @Nested
    @GdlExtension
    class Unweighted {

        // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node { expectedRank: 0.3040965, expectedPersonalizedRank1: 0.17053529152163158 , expectedPersonalizedRank2: 0.017454997930076894 })" +
            ", (b:Node { expectedRank: 3.5604297, expectedPersonalizedRank1: 0.3216114449911402  , expectedPersonalizedRank2: 0.813246950528992    })" +
            ", (c:Node { expectedRank: 3.1757906, expectedPersonalizedRank1: 0.27329311398643763 , expectedPersonalizedRank2: 0.690991752640184    })" +
            ", (d:Node { expectedRank: 0.3625935, expectedPersonalizedRank1: 0.048318333106500536, expectedPersonalizedRank2: 0.041070583050331164 })" +
            ", (e:Node { expectedRank: 0.7503465, expectedPersonalizedRank1: 0.17053529152163158 , expectedPersonalizedRank2: 0.1449550029964717   })" +
            ", (f:Node { expectedRank: 0.3625935, expectedPersonalizedRank1: 0.048318333106500536, expectedPersonalizedRank2: 0.041070583050331164 })" +
            ", (g:Node { expectedRank: 0.15     , expectedPersonalizedRank1: 0.0                 , expectedPersonalizedRank2: 0.0                  })" +
            ", (h:Node { expectedRank: 0.15     , expectedPersonalizedRank1: 0.0                 , expectedPersonalizedRank2: 0.0                  })" +
            ", (i:Node { expectedRank: 0.15     , expectedPersonalizedRank1: 0.0                 , expectedPersonalizedRank2: 0.0                  })" +
            ", (j:Node { expectedRank: 0.15     , expectedPersonalizedRank1: 0.0                 , expectedPersonalizedRank2: 0.0                  })" +
            ", (k:Node { expectedRank: 0.15     , expectedPersonalizedRank1: 0.0                 , expectedPersonalizedRank2: 0.15000000000000002  })" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)" +
            ", (d)-[:TYPE]->(a)" +
            ", (d)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(d)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(b)" +
            ", (f)-[:TYPE]->(e)" +
            ", (g)-[:TYPE]->(b)" +
            ", (g)-[:TYPE]->(e)" +
            ", (h)-[:TYPE]->(b)" +
            ", (h)-[:TYPE]->(e)" +
            ", (i)-[:TYPE]->(b)" +
            ", (i)-[:TYPE]->(e)" +
            ", (j)-[:TYPE]->(e)" +
            ", (k)-[:TYPE]->(e)";

        @Inject
        private TestGraph graph;

        @Test
        void withoutTolerance() {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .concurrency(1)
                .tolerance(0)
                .build();

            var actual = runOnPregel(graph, config)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @ParameterizedTest
        @CsvSource(value = {"0.5, 2", "0.1, 13"})
        void withTolerance(double tolerance, int expectedIterations) {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .concurrency(1)
                .tolerance(tolerance)
                .build();

            var pregelResult = runOnPregel(graph, config);

            // initial iteration is counted extra in Pregel
            assertThat(pregelResult.iterations()).isEqualTo(expectedIterations);
        }

        @ParameterizedTest
        @CsvSource(value = {
            "a;e,expectedPersonalizedRank1",
            "k;b,expectedPersonalizedRank2"
        })
        void withSourceNodes(String sourceNodesString, String expectedPropertyKey) {
            // ids are converted to mapped ids within the algorithms
            var sourceNodeIds = Arrays.stream(sourceNodesString.split(";")).mapToLong(graph::toOriginalNodeId).toArray();

            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .tolerance(0)
                .concurrency(1)
                .build();

            var actual = runOnPregel(graph, config, sourceNodeIds, Mode.PAGE_RANK)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties(expectedPropertyKey);

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void shouldLogProgress() {
            var config = ImmutablePageRankPregelConfig.builder().build();

            var testLogger = new TestProgressLogger(
                graph.nodeCount(),
                "PageRank",
                config.concurrency()
            );

            runOnPregel(graph, config, Mode.PAGE_RANK, testLogger);

            testLogger.getProgresses().forEach(progress -> assertEquals(graph.nodeCount(), progress.get()));

            LongStream.rangeClosed(1, config.maxIterations()).forEach(iteration ->
                assertThat(testLogger.getMessages(TestLog.INFO))
                    // avoid asserting on the thread id
                    .extracting(removingThreadId())
                    .contains(
                        formatWithLocale(
                            "PageRank :: Iteration %d/%d :: Start",
                            iteration,
                            config.maxIterations()
                        ),
                        formatWithLocale(
                            "PageRank :: Iteration %d/%d :: Finished",
                            iteration,
                            config.maxIterations()
                        )
                    )
            );
        }
    }

    @Nested
    @GdlExtension
    class Weighted {
        // https://en.wikipedia.org/wiki/PageRank#/media/File:PageRanks-Example.jpg
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node { expectedRank: 0.24919 })" +
            ", (b:Node { expectedRank: 3.69822 })" +
            ", (c:Node { expectedRank: 3.29307 })" +
            ", (d:Node { expectedRank: 0.58349 })" +
            ", (e:Node { expectedRank: 0.72855 })" +
            ", (f:Node { expectedRank: 0.27385 })" +
            ", (g:Node { expectedRank: 0.15 })" +
            ", (h:Node { expectedRank: 0.15 })" +
            ", (i:Node { expectedRank: 0.15 })" +
            ", (j:Node { expectedRank: 0.15 })" +
            ", (k:Node { expectedRank: 0.15 })" +
            ", (b)-[:TYPE { weight: 1.0,   unnormalizedWeight: 5.0 }]->(c)" +
            ", (c)-[:TYPE { weight: 1.0,   unnormalizedWeight: 10.0 }]->(b)" +
            ", (d)-[:TYPE { weight: 0.2,   unnormalizedWeight: 2.0 }]->(a)" +
            ", (d)-[:TYPE { weight: 0.8,   unnormalizedWeight: 8.0 }]->(b)" +
            ", (e)-[:TYPE { weight: 0.10,  unnormalizedWeight: 1.0 }]->(b)" +
            ", (e)-[:TYPE { weight: 0.70,  unnormalizedWeight: 7.0 }]->(d)" +
            ", (e)-[:TYPE { weight: 0.20,  unnormalizedWeight: 2.0 }]->(f)" +
            ", (f)-[:TYPE { weight: 0.7,   unnormalizedWeight: 7.0 }]->(b)" +
            ", (f)-[:TYPE { weight: 0.3,   unnormalizedWeight: 3.0 }]->(e)" +
            ", (g)-[:TYPE { weight: 0.01,  unnormalizedWeight: 0.1 }]->(b)" +
            ", (g)-[:TYPE { weight: 0.99,  unnormalizedWeight: 9.9 }]->(e)" +
            ", (h)-[:TYPE { weight: 0.5,   unnormalizedWeight: 5.0 }]->(b)" +
            ", (h)-[:TYPE { weight: 0.5,   unnormalizedWeight: 5.0 }]->(e)" +
            ", (i)-[:TYPE { weight: 0.5,   unnormalizedWeight: 5.0 }]->(b)" +
            ", (i)-[:TYPE { weight: 0.5,   unnormalizedWeight: 5.0 }]->(e)" +
            ", (j)-[:TYPE { weight: 1.0,   unnormalizedWeight: 10.0 }]->(e)" +
            ", (k)-[:TYPE { weight: 1.0,   unnormalizedWeight: 10.0 }]->(e)";

        @GdlGraph(graphNamePrefix = "zeroWeights")
        private static final String DB_ZERO_WEIGHTS =
            "CREATE" +
            "  (a:Node { expectedRank: 0.15 })" +
            ", (b:Node { expectedRank: 0.15 })" +
            ", (c:Node { expectedRank: 0.15 })" +
            ", (d:Node { expectedRank: 0.15 })" +
            ", (e:Node { expectedRank: 0.15 })" +
            ", (f:Node { expectedRank: 0.15 })" +
            ", (g:Node { expectedRank: 0.15 })" +
            ", (h:Node { expectedRank: 0.15 })" +
            ", (i:Node { expectedRank: 0.15 })" +
            ", (j:Node { expectedRank: 0.15 })" +
            ", (b)-[:TYPE1 {weight: 0}]->(c)" +
            ", (c)-[:TYPE1 {weight: 0}]->(b)" +
            ", (d)-[:TYPE1 {weight: 0}]->(a)" +
            ", (d)-[:TYPE1 {weight: 0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 0}]->(b)" +
            ", (e)-[:TYPE1 {weight: 0}]->(d)" +
            ", (e)-[:TYPE1 {weight: 0}]->(f)" +
            ", (f)-[:TYPE1 {weight: 0}]->(b)" +
            ", (f)-[:TYPE1 {weight: 0}]->(e)";

        @Inject
        private Graph graph;

        @Inject
        private Graph zeroWeightsGraph;

        @ParameterizedTest
        @ValueSource(strings = {"weight", "unnormalizedWeight"})
        void withWeights(String relationshipWeight) {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .tolerance(0)
                .relationshipWeightProperty(relationshipWeight)
                .concurrency(1)
                .build();

            var actual = runOnPregel(graph, config)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void withZeroWeights() {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .tolerance(0)
                .relationshipWeightProperty("weight")
                .concurrency(1)
                .build();

            var actual = runOnPregel(zeroWeightsGraph, config)
                .scores()
                .asNodeProperties();

            var expected = zeroWeightsGraph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < zeroWeightsGraph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }
    }

    @Nested
    @GdlExtension
    class ArticleRank {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node { expectedRank: 0.19991 })" +
            ", (b:Node { expectedRank: 0.41704 })" +
            ", (c:Node { expectedRank: 0.31791 })" +
            ", (d:Node { expectedRank: 0.18921 })" +
            ", (e:Node { expectedRank: 0.19991 })" +
            ", (f:Node { expectedRank: 0.18921 })" +
            ", (g:Node { expectedRank: 0.15 })" +
            ", (h:Node { expectedRank: 0.15 })" +
            ", (i:Node { expectedRank: 0.15 })" +
            ", (j:Node { expectedRank: 0.15 })" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)" +
            ", (d)-[:TYPE]->(a)" +
            ", (d)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(d)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(b)" +
            ", (f)-[:TYPE]->(e)";

        @GdlGraph(graphNamePrefix = "paper")
        public static final String DB_PAPERS =
            "CREATE" +
            "  (a:Node { expectedRank: 0.75619 })" +
            ", (b:Node { expectedRank: 0.56405 })" +
            ", (c:Node { expectedRank: 0.30635 })" +
            ", (d:Node { expectedRank: 0.22862 })" +
            ", (e:Node { expectedRank: 0.27750 })" +
            ", (f:Node { expectedRank: 0.15000 })" +
            ", (g:Node { expectedRank: 0.15000 })" +
            ", (b)-[:TYPE]->(a)" +
            ", (c)-[:TYPE]->(a)" +
            ", (c)-[:TYPE]->(b)" +
            ", (d)-[:TYPE]->(a)" +
            ", (d)-[:TYPE]->(b)" +
            ", (d)-[:TYPE]->(c)" +
            ", (e)-[:TYPE]->(a)" +
            ", (e)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(c)" +
            ", (e)-[:TYPE]->(d)" +
            ", (f)-[:TYPE]->(b)" +
            ", (f)-[:TYPE]->(e)" +
            ", (g)-[:TYPE]->(b)" +
            ", (g)-[:TYPE]->(e)";

        @Inject
        private Graph graph;

        @Inject
        private Graph paperGraph;

        @Test
        void articleRank() {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .maxIterations(40)
                .tolerance(0)
                .concurrency(1)
                .build();

            var actual = runOnPregel(graph, config, new long[0], Mode.ARTICLE_RANK)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void articleRankOnPaperGraph() {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .maxIterations(20)
                .tolerance(0)
                .dampingFactor(0.85)
                .concurrency(1)
                .build();

            var actual = runOnPregel(paperGraph, config, new long[0], Mode.ARTICLE_RANK)
                .scores()
                .asNodeProperties();

            var expected = paperGraph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < paperGraph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }
    }

    @Nested
    @GdlExtension
    class Normalization {
        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node { expectedL1: 0.04658, expectedL2: 0.09099, expectedMean: -0.15783 })" +
            ", (b:Node { expectedL1: 0.36717, expectedL2: 0.71721, expectedMean:  0.78947 })" +
            ", (c:Node { expectedL1: 0.34073, expectedL2: 0.66557, expectedMean:  0.71136 })" +
            ", (d:Node { expectedL1: 0.04195, expectedL2: 0.08194, expectedMean: -0.17152 })" +
            ", (e:Node { expectedL1: 0.04658, expectedL2: 0.09099, expectedMean: -0.15783 })" +
            ", (f:Node { expectedL1: 0.04195, expectedL2: 0.08194, expectedMean: -0.17152 })" +
            ", (g:Node { expectedL1: 0.02875, expectedL2: 0.05616, expectedMean: -0.21052 })" +
            ", (h:Node { expectedL1: 0.02875, expectedL2: 0.05616, expectedMean: -0.21052 })" +
            ", (i:Node { expectedL1: 0.02875, expectedL2: 0.05616, expectedMean: -0.21052 })" +
            ", (j:Node { expectedL1: 0.02875, expectedL2: 0.05616, expectedMean: -0.21052 })" +
            ", (b)-[:TYPE]->(c)" +
            ", (c)-[:TYPE]->(b)" +
            ", (d)-[:TYPE]->(a)" +
            ", (d)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(b)" +
            ", (e)-[:TYPE]->(d)" +
            ", (e)-[:TYPE]->(f)" +
            ", (f)-[:TYPE]->(b)" +
            ", (f)-[:TYPE]->(e)";

        @Inject
        private Graph graph;

        @ParameterizedTest
        @CsvSource({"L1NORM, expectedL1", "L2NORM, expectedL2", "MEAN, expectedMean"})
        void test(ScalarScaler.Variant variant, String expectedPropertyKey) {
            var config = ImmutablePageRankPregelConfig
                .builder()
                .maxIterations(40)
                .tolerance(0)
                .concurrency(1)
                .normalization(variant)
                .build();

            var actual = runOnPregel(graph, config, Mode.EIGENVECTOR, ProgressLogger.NULL_LOGGER).scores();

            var expected = graph.nodeProperties(expectedPropertyKey);

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.get(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }
    }


    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 2412824L, 2412824L),
            Arguments.of(4, 2412992L, 2412992L),
            Arguments.of(42, 2415120L, 2415120L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        var config = ImmutablePageRankPregelConfig
            .builder()
            .build();

        var nodeCount = 100_000;
        var relationshipCount = nodeCount * 10;

        assertMemoryEstimation(
            () -> new PageRankPregelAlgorithmFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            concurrency,
            expectedMinBytes,
            expectedMaxBytes
        );
    }

    @Test
    void shouldComputeMemoryEstimationFor10BElements() {
        var config = ImmutablePageRankPregelConfig
            .builder()
            .build();

        var nodeCount = 10_000_000_000L;
        var relationshipCount = 10_000_000_000L;
        assertMemoryEstimation(
            () -> new PageRankPregelAlgorithmFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            4,
            241_286_621_632L,
            241_286_621_632L
        );
    }

    PageRankPregelResult runOnPregel(Graph graph, PageRankBaseConfig config) {
        return runOnPregel(graph, config, new long[0], Mode.PAGE_RANK);
    }

    PageRankPregelResult runOnPregel(Graph graph, PageRankBaseConfig config, long[] sourceNodeIds, Mode mode) {
        var configBuilder = ImmutablePageRankPregelConfig.builder()
            .maxIterations(config.maxIterations() + 1)
            .dampingFactor(config.dampingFactor())
            .concurrency(config.concurrency())
            .relationshipWeightProperty(config.relationshipWeightProperty())
            .sourceNodeIds(LongStream.of(sourceNodeIds))
            .tolerance(config.tolerance())
            .isAsynchronous(false);

        return runOnPregel(graph, configBuilder.build(), mode, ProgressLogger.NULL_LOGGER);
    }

    PageRankPregelResult runOnPregel(Graph graph, PageRankPregelConfig config, Mode mode, ProgressLogger progressLogger) {
        return new PageRankPregelAlgorithmFactory<>(mode)
            .build(
                graph,
                config,
                AllocationTracker.empty(),
                progressLogger
            )
            .compute();
    }
}
