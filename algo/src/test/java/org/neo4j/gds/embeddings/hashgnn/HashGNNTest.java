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
package org.neo4j.gds.embeddings.hashgnn;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.neo4j.gds.TestSupport.assertMemoryEstimation;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;

@GdlExtension
class HashGNNTest {

    @GdlGraph(graphNamePrefix = "binary")
    private static final String BINARY_GRAPH =
        "CREATE" +
        "  (a:N {f1: 1, f2: [0.0, 0.0]})" +
        ", (b:N {f1: 0, f2: [1.0, 0.0]})" +
        ", (c:N {f1: 0, f2: [0.0, 1.0]})" +
        ", (b)-[:R]->(a)" +
        ", (b)-[:R]->(c)";

    @GdlGraph(graphNamePrefix = "double")
    private static final String DOUBLE_GRAPH =
        "CREATE" +
        "  (a:N {f1: 1.1, f2: [1.3, 2.0]})" +
        ", (b:N {f1: 1.5, f2: [-3.1, 1.6]})" +
        ", (c:N {f1: -0.6, f2: [0.0, -1.0]})" +
        ", (b)-[:R]->(a)" +
        ", (b)-[:R]->(c)";

    @Inject
    private Graph binaryGraph;

    @Inject
    private IdFunction binaryIdFunction;

    @Inject
    private Graph doubleGraph;

    @Inject
    private IdFunction doubleIdFunction;

    @Test
    void binaryHighEmbeddingDensityLowNeighborInfluence() {
        int embeddingDensity = 200;
        double avgDegree = binaryGraph.relationshipCount() / (double) binaryGraph.nodeCount();
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .neighborInfluence(avgDegree / embeddingDensity)
            .iterations(10)
            .randomSeed(42L)
            .build();
        var result = new HashGNN(binaryGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        //dimension should be equal to dimension of feature input which is 3
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("a")))).containsExactly(1.0, 0.0, 0.0);
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("b")))).containsExactly(1.0, 1.0, 1.0);
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("c")))).containsExactly(0.0, 0.0, 1.0);
    }

    @Test
    void binaryHighEmbeddingDensityHighNeighborInfluence() {
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(200)
            .neighborInfluence(100)
            .iterations(10)
            .randomSeed(42L)
            .build();
        var result = new HashGNN(binaryGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        //dimension should be equal to dimension of feature input which is 3
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("a")))).containsExactly(1.0, 0.0, 0.0);
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("b")))).containsExactly(1.0, 0.0, 1.0);
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("c")))).containsExactly(0.0, 0.0, 1.0);
    }

    @Test
    void binaryLowEmbeddingDensity() {
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(1)
            .iterations(10)
            .randomSeed(42L)
            .build();
        var result = new HashGNN(binaryGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        //dimension should be equal to dimension of feature input which is 3
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("a")))).containsExactly(1.0, 0.0, 0.0);
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("b")))).containsExactly(0.0, 0.0, 1.0);
        assertThat(result.get(binaryGraph.toMappedNodeId(binaryIdFunction.of("c")))).containsExactly(0.0, 0.0, 1.0);
    }

    static Stream<Arguments> determinismParams() {
        return TestSupport.crossArguments(
            // concurrency
            () -> Stream.of(Arguments.of(1), Arguments.of(4)),
            // binarize
            () -> Stream.of(Arguments.of(true), Arguments.of(false)),
            // dimension reduction
            () -> Stream.of(Arguments.of(true), Arguments.of(false))
        );
    }

    @ParameterizedTest
    @MethodSource("determinismParams")
    void shouldBeDeterministic(int concurrency, boolean binarize, boolean dimReduce) {
        var configBuilder = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(2)
            .concurrency(concurrency)
            .iterations(1)
            .randomSeed(42L);

        if (binarize) {
            configBuilder.binarizeFeatures(Map.of("dimension", 12, "densityLevel", 6));
        }

        if (dimReduce) {
            configBuilder.outputDimension(1);
        }

        var config = configBuilder.build();

        var result1 = new HashGNN(binaryGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        var result2 = new HashGNN(binaryGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();

        for (int i = 0; i < result1.size(); i++) {
            assertThat(result1.get(i)).containsExactly(result2.get(i));
        }
    }

    @Test
    void shouldRunOnDoublesAndBeDeterministicEqualScaledNeighborInfluence() {
        int embeddingDensity = 200;
        int binarizationDimension = 16;

        var configBuilder = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .binarizeFeatures(Map.of("dimension", binarizationDimension, "densityLevel", 2))
            .iterations(1)
            .randomSeed(43L);
        var configBefore = configBuilder.neighborInfluence(0).build();
        var resultBefore = new HashGNN(doubleGraph, configBefore, ProgressTracker.NULL_TRACKER).compute().embeddings();


        var avgDegree = doubleGraph.relationshipCount() / (double) doubleGraph.nodeCount();
        var config = configBuilder.neighborInfluence(avgDegree / embeddingDensity).build();
        var result = new HashGNN(doubleGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        // because of high neighbor influence and high embeddingDensity, we expect the node `b` to have the union of features of its neighbors
        // the neighbors are expected to have the same features as their initial projection


        double[] embeddingB = result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("b")));
        double[] embeddingABefore = resultBefore.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("a")));
        double[] embeddingCBefore = resultBefore.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("c")));

        var bHasUniqueFeature = false;
        for (int component = 0; component < binarizationDimension; component++) {
            assertThat(embeddingB[component]).isGreaterThanOrEqualTo(Math.max(
                embeddingABefore[component],
                embeddingCBefore[component]
            ));
            if (embeddingB[component] > Math.max(embeddingABefore[component], embeddingCBefore[component])) {
                bHasUniqueFeature = true;
            }
        }

        assertThat(result.get(0).length).isEqualTo(binarizationDimension);
        assertThat(bHasUniqueFeature).isTrue();
    }

    @Test
    void shouldRunOnDoublesAndBeDeterministicHighNeighborInfluence() {
        int embeddingDensity = 50;
        int binarizationDimension = 16;

        var configBuilder = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .binarizeFeatures(Map.of("dimension", binarizationDimension, "densityLevel", 2))
            .iterations(1)
            .randomSeed(3L);
        var configBefore = configBuilder.neighborInfluence(0).build();
        var resultBefore = new HashGNN(doubleGraph, configBefore, ProgressTracker.NULL_TRACKER).compute().embeddings();


        var config = configBuilder
            .neighborInfluence(4)
            .build();
        var result = new HashGNN(doubleGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        // because of equal neighbor and self influence and high embeddingDensity, we expect the node `b` to have the union of features of its neighbors plus some of its own features
        // the neighbors are expected to have the same features as their initial projection
        double[] embeddingB = result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("b")));
        double[] embeddingABefore = resultBefore.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("a")));
        double[] embeddingCBefore = resultBefore.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("c")));
        for (int component = 0; component < binarizationDimension; component++) {
            assertThat(embeddingB[component]).isEqualTo(Math.max(
                embeddingABefore[component],
                embeddingCBefore[component]
            ));
        }

        assertThat(result.get(0).length).isEqualTo(binarizationDimension);
    }

    @Test
    void outputDimensionIsApplied() {
        int embeddingDensity = 200;
        double avgDegree = binaryGraph.relationshipCount() / (double) binaryGraph.nodeCount();
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .neighborInfluence(avgDegree / embeddingDensity)
            .iterations(10)
            .randomSeed(42L)
            .outputDimension(42)
            .build();
        var result = new HashGNN(binaryGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        //dimension should be equal to dimension of feature input which is 3
        assertThat(result.get(0).length).isEqualTo(42);
        assertThat(result.get(1).length).isEqualTo(42);
        assertThat(result.get(2).length).isEqualTo(42);
    }

    @ParameterizedTest
    @CsvSource(value = {
        // BASE
        "    10,  4,  10_000, 20_000, 1,  86_055_752",

        // Should increase fairly little with higher density
        "   100,  4,  10_000, 20_000, 1,  90_515_432",

        // Should increase fairly little with more iterations
        "    10, 16,  10_000, 20_000, 1,  87_542_312",

        // Should increase almost linearly with node count
        "    10,  4, 100_000, 20_000, 1, 856_096_112",

        // Should be unaffected by relationship count
        "    10,  4,  10_000, 80_000, 1,  86_055_752",

        // Should be unaffected by concurrency
        "    10,  4,  10_000, 20_000, 8,  86_055_752",
    })
        void shouldEstimateMemory(
        int embeddingDensity,
        int iterations,
        long nodeCount,
        long relationshipCount,
        int concurrency,
        long expectedMemory
    ) {
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .iterations(iterations)
            .build();

        assertMemoryEstimation(
            () -> new HashGNNFactory<>().memoryEstimation(config),
            nodeCount,
            relationshipCount,
            concurrency,
            MemoryRange.of(expectedMemory)
        );
    }

    @Test
    void shouldLogProgress() {
        int embeddingDensity = 200;
        double avgDegree = binaryGraph.relationshipCount() / (double) binaryGraph.nodeCount();

        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)

            .neighborInfluence(avgDegree / embeddingDensity)
            .iterations(2)
            .randomSeed(42L)
            .outputDimension(2)
            .build();

        var factory = new HashGNNFactory<>();

        var progressTask = factory.progressTask(binaryGraph, config);
        var log = Neo4jProxy.testLog();
        ;
        var progressTracker = new TaskProgressTracker(progressTask, log, 4, EmptyTaskRegistryFactory.INSTANCE);

        factory
            .build(binaryGraph, config, progressTracker)
            .compute();

        Assertions.assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "HashGNN :: Start",
                "HashGNN :: Extract raw node property features :: Start",
                "HashGNN :: Extract raw node property features 100%",
                "HashGNN :: Extract raw node property features :: Finished",
                "HashGNN :: Precompute hashes :: Start",
                "HashGNN :: Precompute hashes 100%",
                "HashGNN :: Precompute hashes :: Finished",
                "HashGNN :: Propagate embeddings :: Start",
                "HashGNN :: Propagate embeddings :: Propagate embeddings iteration 1 of 2 :: Start",
                "HashGNN :: Propagate embeddings :: Propagate embeddings iteration 1 of 2 100%",
                "HashGNN :: Propagate embeddings :: Propagate embeddings iteration 1 of 2 :: Finished",
                "HashGNN :: Propagate embeddings :: Propagate embeddings iteration 2 of 2 :: Start",
                "HashGNN :: Propagate embeddings :: Propagate embeddings iteration 2 of 2 100%",
                "HashGNN :: Propagate embeddings :: Propagate embeddings iteration 2 of 2 :: Finished",
                "HashGNN :: Propagate embeddings :: Finished",
                "HashGNN :: Densify output embeddings :: Start",
                "HashGNN :: Densify output embeddings 100%",
                "HashGNN :: Densify output embeddings :: Finished",
                "HashGNN :: Finished"
            );
    }
}
