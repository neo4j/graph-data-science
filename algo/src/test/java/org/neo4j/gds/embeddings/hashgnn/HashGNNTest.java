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

import com.carrotsearch.hppc.BitSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.TestSupport;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

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
    void shouldRunOnDoublesAndBeDeterministicHighNeighborInfluence() {
        int embeddingDensity = 20;
        int binarizationDimension = 16;
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .binarizeFeatures(Map.of("dimension", binarizationDimension, "densityLevel", 2))
            .iterations(1)
            .neighborInfluence(4)
            .randomSeed(43L)
            .build();
        var result = new HashGNN(doubleGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        // because of high neighbor influence and high embeddingDensity, we expect the node `b` to have the union of features of its neighbors
        // the neighbors are expected to have the same features as their initial projection
        assertThat(result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("a")))).containsExactly(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0);
        assertThat(result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("b")))).containsExactly(0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 0.0);
        assertThat(result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("c")))).containsExactly(0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0);

        assertThat(result.get(0).length).isEqualTo(binarizationDimension);
    }

    @Test
    void shouldRunOnDoublesAndBeDeterministicLowNeighborInfluence() {
        int embeddingDensity = 20;
        int binarizationDimension = 16;
        double avgDegree = binaryGraph.relationshipCount() / (double) binaryGraph.nodeCount();
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("f1", "f2"))
            .embeddingDensity(embeddingDensity)
            .binarizeFeatures(Map.of("dimension", binarizationDimension, "densityLevel", 2))
            .neighborInfluence(0.01)
            .iterations(1)
            .randomSeed(1L)
            .build();
        var result = new HashGNN(doubleGraph, config, ProgressTracker.NULL_TRACKER).compute().embeddings();
        // because of equal neighbor and self influence and high embeddingDensity, we expect the node `b` to have the union of features of its neighbors plus some of its own features
        // the neighbors are expected to have the same features as their initial projection
        assertThat(result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("a")))).containsExactly(0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0);
        assertThat(result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("b")))).containsExactly(0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 1.0);
        assertThat(result.get(doubleGraph.toMappedNodeId(doubleIdFunction.of("c")))).containsExactly(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0);

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

    @Test
    void shouldComputeHashesFromTriple() {
        int AMBIENT_DIMENSION = 10;

        var rng = new SplittableRandom();
        int c = rng.nextInt(2, 100);
        int a = rng.nextInt(c - 1) + 1;
        int b = rng.nextInt(c - 1) + 1;

        var hashTriple = ImmutableHashTriple.of(a, b, c);
        var hashes = HashGNNCompanion.HashTriple.computeHashesFromTriple(AMBIENT_DIMENSION, hashTriple);

        assertThat(hashes.length).isEqualTo(AMBIENT_DIMENSION);
        assertThat(hashes).containsAnyOf(IntStream.range(0, c).toArray());
    }

    @Test
    void shouldHashArgMin() {
        var rng = new SplittableRandom();

        var bitSet = new BitSet(10);
        bitSet.set(3);
        bitSet.set(9);

        var hashes = IntStream.generate(() -> rng.nextInt(0, Integer.MAX_VALUE)).limit(10).toArray();
        var minArgMin = new HashGNN.MinAndArgmin(Integer.MAX_VALUE, -1);

        HashGNNCompanion.hashArgMin(bitSet, hashes, minArgMin);

        assertThat(minArgMin.min).isEqualTo(Math.min(hashes[3], hashes[9]));
        assertThat(minArgMin.argMin).isEqualTo(hashes[3] <= hashes[9] ? 3 : 9);
    }
}
