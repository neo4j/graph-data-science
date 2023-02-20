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
package org.neo4j.gds.pagerank;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.pagerank.PageRankAlgorithmFactory.Mode;
import org.neo4j.gds.scaling.ScalarScaler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ExtendWith(SoftAssertionsExtension.class)
class PageRankTest {

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
                .maxIterations(41)
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
            var sourceNodeIds = Arrays.stream(sourceNodesString.split(";"))
                .map(graph::toOriginalNodeId)
                .collect(Collectors.toList());

            var config = ImmutablePageRankConfig.builder()
                .maxIterations(41)
                .tolerance(0)
                .concurrency(1)
                .sourceNodes(sourceNodeIds)
                .build();

            var actual = runOnPregel(graph, config)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties(expectedPropertyKey);

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void shouldLogProgress() {
            var maxIterations = 10;
            var config = ImmutablePageRankConfig.builder()
                .maxIterations(maxIterations)
                .build();

            var progressTask = PageRankAlgorithmFactory.pagerankProgressTask(graph, config);
            var log = Neo4jProxy.testLog();
            var progressTracker = new TestProgressTracker(
                progressTask,
                log,
                config.concurrency(),
                EmptyTaskRegistryFactory.INSTANCE
            );

            runOnPregel(graph, config, Mode.PAGE_RANK, progressTracker);

            var progresses = progressTracker.getProgresses().stream()
                .filter(it -> it.get() > 0)
                .collect(Collectors.toList());

            // the algorithm doesn't converge in `maxIterations`, so we should have at least that many non-zero progresses
            assertThat(progresses.size()).isGreaterThanOrEqualTo(maxIterations);

            progresses.forEach(progress -> {
                // the first iteration will compute degree centrality and therefore log nodeCount messages twice
                assertThat(progress.get()).isIn(List.of(graph.nodeCount(), graph.nodeCount() * 2));
            });

            var messages = log.getMessages(TestLog.INFO);

            LongStream.rangeClosed(1, config.maxIterations()).forEach(iteration ->
                assertThat(messages)
                    // avoid asserting on the thread id
                    .extracting(removingThreadId())
                    .contains(
                        formatWithLocale(
                            "PageRank :: Compute iteration %d of %d :: Start",
                            iteration,
                            config.maxIterations()
                        ),
                        formatWithLocale(
                            "PageRank :: Compute iteration %d of %d :: Finished",
                            iteration,
                            config.maxIterations()
                        ),
                        formatWithLocale(
                            "PageRank :: Master compute iteration %d of %d :: Start",
                            iteration,
                            config.maxIterations()
                        ),
                        formatWithLocale(
                            "PageRank :: Master compute iteration %d of %d :: Finished",
                            iteration,
                            config.maxIterations()
                        )
                    )
            );
            assertThat(messages)
                .extracting(removingThreadId())
                .contains(
                    "PageRank :: Start",
                    "PageRank :: Finished"
                );
        }

        @Test
        void checkTerminationFlag() {
            var config = ImmutablePageRankStreamConfig.builder()
                .maxIterations(40)
                .concurrency(1)
                .build();

            var algo = new PageRankAlgorithmFactory<>(Mode.PAGE_RANK)
                .build(
                    graph,
                    config,
                    ProgressTracker.NULL_TRACKER
                );

            algo.setTerminationFlag(() -> false);

            TestSupport.assertTransactionTermination(algo::compute);
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
                .maxIterations(41)
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
            "  (a:Node { expectedRank: 0.20720 })" +
            ", (b:Node { expectedRank: 0.47091 })" +
            ", (c:Node { expectedRank: 0.36067 })" +
            ", (d:Node { expectedRank: 0.19515 })" +
            ", (e:Node { expectedRank: 0.20720 })" +
            ", (f:Node { expectedRank: 0.19515 })" +
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
            "  (a:Node { expectedRank: 0.34627 })" +
            ", (b:Node { expectedRank: 0.31950 })" +
            ", (c:Node { expectedRank: 0.21092 })" +
            ", (d:Node { expectedRank: 0.18028 })" +
            ", (e:Node { expectedRank: 0.21375 })" +
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
        void articleRank(SoftAssertions softly) {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .maxIterations(40)
                .tolerance(0)
                .concurrency(1)
                .build();

            var actual = runOnPregel(graph, config, Mode.ARTICLE_RANK)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                softly.assertThat(actual.doubleValue(nodeId))
                    .isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void articleRankOnPaperGraph(SoftAssertions softly) {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .maxIterations(20)
                .tolerance(0)
                .dampingFactor(0.85)
                .concurrency(1)
                .build();

            var actual = runOnPregel(paperGraph, config, Mode.ARTICLE_RANK)
                .scores()
                .asNodeProperties();

            var expected = paperGraph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < paperGraph.nodeCount(); nodeId++) {
                softly.assertThat(actual.doubleValue(nodeId))
                    .isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }
    }

    @Nested
    @GdlExtension
    class Eigenvector {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:Node { expectedRank: 0.01262, expectedWeightedRank: 0.00210, expectedPersonalizedRank:  0.00997 })" +
            ", (b:Node { expectedRank: 0.71623, expectedWeightedRank: 0.70774, expectedPersonalizedRank:  0.70735 })" +
            ", (c:Node { expectedRank: 0.69740, expectedWeightedRank: 0.70645, expectedPersonalizedRank:  0.70678 })" +
            ", (d:Node { expectedRank: 0.01262, expectedWeightedRank: 0.00172, expectedPersonalizedRank:  0.00056 })" +
            ", (e:Node { expectedRank: 0.01262, expectedWeightedRank: 0.00210, expectedPersonalizedRank:  0.0     })" +
            ", (f:Node { expectedRank: 0.01262, expectedWeightedRank: 0.00172, expectedPersonalizedRank:  0.0     })" +
            ", (g:Node { expectedRank: 0.0    , expectedWeightedRank: 0.0    , expectedPersonalizedRank:  0.0     })" +
            ", (h:Node { expectedRank: 0.0    , expectedWeightedRank: 0.0    , expectedPersonalizedRank:  0.0     })" +
            ", (i:Node { expectedRank: 0.0    , expectedWeightedRank: 0.0    , expectedPersonalizedRank:  0.0     })" +
            ", (j:Node { expectedRank: 0.0    , expectedWeightedRank: 0.0    , expectedPersonalizedRank:  0.0     })" +
            ", (b)-[:TYPE { weight: 1.0 } ]->(c)" +
            ", (c)-[:TYPE { weight: 3.0 } ]->(b)" +
            ", (d)-[:TYPE { weight: 5.0 } ]->(a)" +
            ", (d)-[:TYPE { weight: 5.0 } ]->(b)" +
            ", (e)-[:TYPE { weight: 4.0 } ]->(b)" +
            ", (e)-[:TYPE { weight: 4.0 } ]->(d)" +
            ", (e)-[:TYPE { weight: 4.0 } ]->(f)" +
            ", (f)-[:TYPE { weight: 10.0 } ]->(b)" +
            ", (f)-[:TYPE { weight: 10.0 } ]->(e)";

        @Inject
        private Graph graph;

        @Inject
        IdFunction idFunction;

        @Test
        void eigenvector() {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .maxIterations(40)
                .tolerance(0)
                .concurrency(1)
                .build();

            var actual = runOnPregel(graph, config, Mode.EIGENVECTOR)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void weighted() {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .relationshipWeightProperty("weight")
                .maxIterations(10)
                .tolerance(0)
                .concurrency(1)
                .build();

            var actual = runOnPregel(graph, config, Mode.EIGENVECTOR)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedWeightedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }

        @Test
        void withSourceNodes() {
            var config = ImmutablePageRankStreamConfig
                .builder()
                .maxIterations(10)
                .tolerance(0.1)
                .concurrency(1)
                .addSourceNode(idFunction.of("d"))
                .build();

            var actual = runOnPregel(graph, config, Mode.EIGENVECTOR)
                .scores()
                .asNodeProperties();

            var expected = graph.nodeProperties("expectedPersonalizedRank");

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.doubleValue(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }
    }

    @Nested
    @GdlExtension
    class Scaling {
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
        void test(String scalerName, String expectedPropertyKey) {
            var config = ImmutablePageRankConfig
                .builder()
                .maxIterations(40)
                .tolerance(0)
                .concurrency(1)
                .scaler(ScalarScaler.ScalerFactory.parse(scalerName))
                .build();

            var actual = runOnPregel(graph, config).scores();

            var expected = graph.nodeProperties(expectedPropertyKey);

            for (int nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
                assertThat(actual.get(nodeId)).isEqualTo(expected.doubleValue(nodeId), within(SCORE_PRECISION));
            }
        }
    }


    static Stream<Arguments> expectedMemoryEstimation() {
        return Stream.of(
            Arguments.of(1, 2412832L, 2412832L),
            Arguments.of(4, 2413000L, 2413000L),
            Arguments.of(42, 2415128L, 2415128L)
        );
    }

    @ParameterizedTest
    @MethodSource("expectedMemoryEstimation")
    void shouldComputeMemoryEstimation(int concurrency, long expectedMinBytes, long expectedMaxBytes) {
        var config = ImmutablePageRankConfig
            .builder()
            .build();

        var nodeCount = 100_000;
        var relationshipCount = nodeCount * 10;

        assertMemoryEstimation(
            () -> new PageRankAlgorithmFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            concurrency,
            MemoryRange.of(expectedMinBytes, expectedMaxBytes)
        );
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void parallelExecution(Mode mode) {
        var graph = RandomGraphGenerator.builder()
            .nodeCount(40_000)
            .averageDegree(5)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .build()
            .generate();

        var configBuilder = ImmutablePageRankConfig.builder();

        var singleThreaded = runOnPregel(graph, configBuilder.concurrency(1).build(), mode).scores();
        var multiThreaded = runOnPregel(graph, configBuilder.concurrency(4).build(), mode).scores();

        for (long nodeId = 0; nodeId < graph.nodeCount(); nodeId++) {
            assertThat(singleThreaded.get(nodeId)).isEqualTo(multiThreaded.get(nodeId), Offset.offset(1e-5));
        }
    }

    @Test
    void shouldComputeMemoryEstimationFor10BElements() {
        var config = ImmutablePageRankConfig
            .builder()
            .build();

        var nodeCount = 10_000_000_000L;
        var relationshipCount = 10_000_000_000L;
        assertMemoryEstimation(
            () -> new PageRankAlgorithmFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            4,
            MemoryRange.of(241_286_621_640L, 241_286_621_640L)
        );
    }

    PageRankResult runOnPregel(Graph graph, PageRankConfig config) {
        return runOnPregel(graph, config, Mode.PAGE_RANK);
    }

    PageRankResult runOnPregel(Graph graph, PageRankConfig config, Mode mode) {
        return runOnPregel(graph, config, mode, ProgressTracker.NULL_TRACKER);
    }

    PageRankResult runOnPregel(Graph graph, PageRankConfig config, Mode mode, ProgressTracker progressTracker) {
        return new PageRankAlgorithmFactory<>(mode)
            .build(
                graph,
                config,
                progressTracker
            )
            .compute();
    }
}
