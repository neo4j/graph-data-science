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
package org.neo4j.gds.embeddings.fastrp;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.ArrayIdMap;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;
import org.neo4j.gds.core.utils.shuffle.ShuffleUtil;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.gds.TestSupport.assertMemoryRange;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.l2Normalize;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
public class FastRPTest {

    private static final int DEFAULT_EMBEDDING_DIMENSION = 128;
    private static final FastRPBaseConfig DEFAULT_CONFIG = FastRPBaseConfig.builder()
        .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
        .propertyRatio(0.5)
        .featureProperties(List.of("f1", "f2", "f3"))
        .addIterationWeight(1.0D)
        .randomSeed(42L)
        .build();

    @GdlGraph(graphNamePrefix = "array")
    private static final String X =
        "CREATE" +
        "  (a:Node1 {f: [0.4, 1.3, 1.4]})" +
        ", (b:Node1 {f: [2.1, 0.5, 1.8]})" +
        ", (c:Node2 {f: [-0.3, 0.8, 2.8]})" +
        ", (d:Isolated {f: [2.5, 8.1, 1.3]})" +
        ", (e:Isolated {f: [0.6, 0.5, 5.2]})" +
        ", (a)-[:REL {weight: 2.0}]->(b)" +
        ", (b)-[:REL {weight: 1.0}]->(a)" +
        ", (a)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(a)" +
        ", (b)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(b)";

    @GdlGraph(graphNamePrefix = "scalar")
    private static final String Y =
        "CREATE" +
        "  (a:Node1 {f1: 0.4, f2: 1.3, f3: 1.4})" +
        ", (b:Node1 {f1: 2.1, f2: 0.5, f3: 1.8})" +
        ", (c:Node2 {f1: -0.3, f2: 0.8, f3: 2.8})" +
        ", (d:Isolated {f1: 2.5, f2: 8.1, f3: 1.3})" +
        ", (e:Isolated {f1: -0.6, f2: 0.5, f3: 5.2]})" +
        ", (a)-[:REL {weight: 2.0}]->(b)" +
        ", (b)-[:REL {weight: 1.0}]->(a)" +
        ", (a)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(a)" +
        ", (b)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(b)";

    @Inject
    Graph scalarGraph;

    @Inject
    Graph arrayGraph;

    @Inject
    GraphStore scalarGraphStore;

    @Test
    void shouldYieldSameResultsForScalarAndArrayProperties() {
        assert arrayGraph.nodeCount() == scalarGraph.nodeCount();
        var arrayProperties = List.of("f");
        var arrayEmbeddings = embeddings(arrayGraph, arrayProperties);
        var scalarProperties = List.of("f1", "f2", "f3");
        var scalarEmbeddings = embeddings(scalarGraph, scalarProperties);
        for (int i = 0; i < arrayGraph.nodeCount(); i++) {
            assertThat(arrayEmbeddings.get(i)).contains(scalarEmbeddings.get(i));
        }
    }

    @Test
    void shouldSwapInitialRandomVectors() {
        var graph = scalarGraphStore.getGraph(
            NodeLabel.of("Node1"),
            RelationshipType.of("REL"),
            Optional.empty()
        );

        FastRP fastRP = new FastRP(
            graph,
            DEFAULT_CONFIG,
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        fastRP.initDegreePartition();
        fastRP.initPropertyVectors();
        fastRP.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 2);
        fastRP.currentEmbedding(-1).copyTo(randomVectors, 2);
        fastRP.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = fastRP.embeddings();

        float[] expected = randomVectors.get(1);
        l2Normalize(expected);

        assertThat(embeddings.get(0)).isEqualTo(expected);
    }

    @Test
    void shouldAverageNeighbors() {
        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        FastRP fastRP = new FastRP(
            graph,
            DEFAULT_CONFIG,
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        fastRP.initDegreePartition();
        fastRP.initPropertyVectors();
        fastRP.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 3);
        fastRP.currentEmbedding(-1).copyTo(randomVectors, 3);
        fastRP.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = fastRP.embeddings();

        float[] expected = new float[DEFAULT_EMBEDDING_DIMENSION];
        for (int i = 0; i < DEFAULT_EMBEDDING_DIMENSION; i++) {
            expected[i] = (randomVectors.get(1)[i] + randomVectors.get(2)[i]) / 2.0f;
        }
        l2Normalize(expected);

        assertThat(embeddings.get(0)).containsExactly(expected);
    }

    @Test
    void shouldAddInitialVectors() {
        var embeddingDimension = 6;
        var config = FastRPBaseConfig.builder()
            .embeddingDimension(embeddingDimension)
            .propertyRatio(0.5)
            .featureProperties(List.of("f1", "f2", "f3"))
            .nodeSelfInfluence(0.6)
            .addIterationWeight(0.0D)
            .randomSeed(42L)
            .build();

        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        FastRP fastRP = new FastRP(
            graph,
            config,
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        fastRP.initDegreePartition();
        // needed to avoid NPE. the randomvectors are overwritten below.
        fastRP.initRandomVectors();

        var initialRandomVectors = fastRP.currentEmbedding(-1);
        var initial0 = new float[embeddingDimension];
        var initial1 = new float[embeddingDimension];
        var initial2 = new float[embeddingDimension];
        initial0[0] = 1.0f;
        initial0[3] = -1.0f;
        initial1[1] = 2.4f;
        initial1[2] = -0.5f;
        initial2[5] = -3.0f;
        initial2[4] = -0.5f;
        initialRandomVectors.set(0, initial0);
        initialRandomVectors.set(1, initial1);
        initialRandomVectors.set(2, initial2);

        fastRP.addInitialVectorsToEmbedding();
        fastRP.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = fastRP.embeddings();


        var expected0 = new float[embeddingDimension];
        var expected1 = new float[embeddingDimension];
        var expected2 = new float[embeddingDimension];
        var scale0 = config.nodeSelfInfluence().floatValue() / (float) Math.sqrt(2);
        var scale1 = config.nodeSelfInfluence().floatValue() / (float) Math.sqrt(2.4 * 2.4 + 0.5 * 0.5);
        var scale2 = config.nodeSelfInfluence().floatValue() / (float) Math.sqrt(3.0 * 3.0 + 0.5 * 0.5);
        expected0[0] = scale0;
        expected0[3] = -1.0f * scale0;
        expected1[1] = 2.4f * scale1;
        expected1[2] = -0.5f * scale1;
        expected2[5] = -3.0f * scale2;
        expected2[4] = -0.5f * scale2;

        assertThat(embeddings.get(0)).containsExactly(expected0, Offset.offset(1e-6f));
        assertThat(embeddings.get(1)).containsExactly(expected1, Offset.offset(1e-6f));
        assertThat(embeddings.get(2)).containsExactly(expected2, Offset.offset(1e-6f));
    }

    @Test
    void shouldInitialisePropertyEmbeddingsCorrectly() {
        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        FastRP fastRP = new FastRP(
            graph,
            DEFAULT_CONFIG,
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        fastRP.initPropertyVectors();

        // these asserted values were copied from the algorithm output. testing for stability.
        var expectedProp1 = new float[]{0.0f, -0.21650635f, 0.0f, -0.21650635f, 0.21650635f, -0.21650635f, 0.0f, 0.0f, -0.21650635f, 0.21650635f, 0.0f, 0.21650635f, 0.21650635f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, -0.21650635f, -0.21650635f, 0.0f, 0.0f, -0.21650635f, 0.0f, -0.21650635f, 0.21650635f, 0.0f, 0.21650635f, -0.21650635f, 0.0f, 0.0f, 0.21650635f, 0.0f, -0.21650635f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, 0.0f, 0.0f, 0.21650635f, -0.21650635f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, 0.0f, -0.21650635f, 0.0f, 0.21650635f, -0.21650635f, 0.0f, 0.0f, -0.21650635f, 0.0f};
        var expectedProp2 = new float[]{0.21650635f, 0.0f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.0f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, 0.21650635f, 0.0f, 0.0f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.21650635f, -0.21650635f, -0.21650635f, 0.0f, 0.21650635f, -0.21650635f, 0.0f, 0.21650635f, 0.0f, 0.0f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, 0.21650635f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f};
        var expectedProp3 = new float[]{0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.21650635f, 0.21650635f, 0.0f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.21650635f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.21650635f, 0.0f, 0.0f};

        assertArrayEquals(expectedProp1, fastRP.propertyVectors()[0]);
        assertArrayEquals(expectedProp2, fastRP.propertyVectors()[1]);
        assertArrayEquals(expectedProp3, fastRP.propertyVectors()[2]);

        fastRP.initRandomVectors();

        // these were obtained by computing a matrix product P * V where
        //    P is the propertyDimension x inputDimension matrix with expected propertyVectors as columns
        //    V is the inputDimension x nodeCount matrix of property values in the graph
        var initialPropComponentOfNodeVector1 = new float[]{0.584567145f, -0.08660254f, -0.281458255f, -0.08660254f, 0.08660254f, -0.08660254f, 0.30310888999999996f, 0.584567145f, 0.21650634999999996f, 0.08660254f, 0.30310888999999996f, 0.368060795f, 0.08660254f, -0.08660254f, 0.0f, 0.0f, 0.281458255f, 0.0f, -0.368060795f, -0.08660254f, -0.08660254f, 0.0f, 0.0f, -0.08660254f, -0.281458255f, 0.19485571499999998f, 0.08660254f, 0.0f, -0.19485571499999998f, -0.08660254f, 0.0f, 0.0f, 0.368060795f, -0.281458255f, -0.368060795f, 0.21650634999999996f, 0.584567145f, -0.281458255f, 0.0f, 0.281458255f, -0.08660254f, 0.0f, -0.02165063499999997f, -0.21650634999999996f, -0.08660254f, 0.08660254f, 0.0f, 0.0f, 0.0f, 0.08660254f, 0.0f, 0.0f, 0.0f, 0.0f, -0.08660254f, -0.30310888999999996f, -0.08660254f, -0.281458255f, 0.368060795f, -0.368060795f, 0.0f, 0.30310888999999996f, -0.08660254f, 0.0f};
        var initialPropComponentOfNodeVector2 = new float[]{0.497964605f, -0.454663335f, -0.108253175f, -0.454663335f, 0.454663335f, -0.454663335f, 0.38971142999999997f, 0.497964605f, -0.06495190500000002f, 0.454663335f, 0.38971142999999997f, 0.56291651f, 0.454663335f, -0.454663335f, 0.0f, 0.0f, 0.108253175f, 0.0f, -0.56291651f, -0.454663335f, -0.454663335f, 0.0f, 0.0f, -0.454663335f, -0.108253175f, -0.34641016f, 0.454663335f, 0.0f, 0.34641016f, -0.454663335f, 0.0f, 0.0f, 0.56291651f, -0.108253175f, -0.56291651f, -0.06495190500000002f, 0.497964605f, -0.108253175f, 0.0f, 0.108253175f, -0.454663335f, 0.0f, -0.281458255f, 0.06495190500000002f, -0.454663335f, 0.454663335f, 0.0f, 0.0f, 0.0f, 0.454663335f, 0.0f, 0.0f, 0.0f, 0.0f, -0.454663335f, -0.38971142999999997f, -0.454663335f, -0.108253175f, 0.56291651f, -0.56291651f, 0.0f, 0.38971142999999997f, -0.454663335f, 0.0f};
        var initialPropComponentOfNodeVector3 = new float[]{0.7794228599999999f, 0.06495190499999999f, -0.17320508f, 0.06495190499999999f, -0.06495190499999999f, 0.06495190499999999f, 0.6062177799999999f, 0.7794228599999999f, 0.671169685f, -0.06495190499999999f, 0.6062177799999999f, 0.10825317500000001f, -0.06495190499999999f, 0.06495190499999999f, 0.0f, 0.0f, 0.17320508f, 0.0f, -0.10825317500000001f, 0.06495190499999999f, 0.06495190499999999f, 0.0f, 0.0f, 0.06495190499999999f, -0.17320508f, 0.238156985f, -0.06495190499999999f, 0.0f, -0.238156985f, 0.06495190499999999f, 0.0f, 0.0f, 0.10825317500000001f, -0.17320508f, -0.10825317500000001f, 0.671169685f, 0.7794228599999999f, -0.17320508f, 0.0f, 0.17320508f, 0.06495190499999999f, 0.0f, -0.4330126999999999f, -0.671169685f, 0.06495190499999999f, -0.06495190499999999f, 0.0f, 0.0f, 0.0f, -0.06495190499999999f, 0.0f, 0.0f, 0.0f, 0.0f, 0.06495190499999999f, -0.6062177799999999f, 0.06495190499999999f, -0.17320508f, 0.10825317500000001f, -0.10825317500000001f, 0.0f, 0.6062177799999999f, 0.06495190499999999f, 0.0f};

        assertThat(initialPropComponentOfNodeVector1)
            .contains(
                takeLastElements(fastRP.currentEmbedding(-1).get(0), DEFAULT_CONFIG.propertyDimension()),
                Offset.offset(1e-6f)
            );
        assertThat(initialPropComponentOfNodeVector2)
            .contains(
                takeLastElements(fastRP.currentEmbedding(-1).get(1), DEFAULT_CONFIG.propertyDimension()),
                Offset.offset(1e-6f)
            );
        assertThat(initialPropComponentOfNodeVector3)
            .contains(
                takeLastElements(fastRP.currentEmbedding(-1).get(2), DEFAULT_CONFIG.propertyDimension()),
                Offset.offset(1e-6f)
            );
    }

    @Test
    void shouldBeDeterministicInParallel() {
        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        var configBuilder = FastRPBaseConfig.builder()
            .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
            .propertyRatio(0.5)
            .featureProperties(List.of("f1", "f2", "f3"))
            .addIterationWeight(1.0D)
            .minBatchSize(1)
            .randomSeed(42L);

        FastRP concurrentFastRP = new FastRP(
            graph,
            configBuilder.concurrency(4).build(),
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        concurrentFastRP.compute();
        HugeObjectArray<float[]> concurrentEmbeddings = concurrentFastRP.embeddings();

        FastRP sequentialFastRP = new FastRP(
            graph,
            configBuilder.concurrency(1).build(),
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        sequentialFastRP.compute();
        HugeObjectArray<float[]> sequentialEmbeddings = sequentialFastRP.embeddings();

        graph.forEachNode(nodeId -> {
            assertThat(concurrentEmbeddings.get(nodeId)).containsExactly(sequentialEmbeddings.get(nodeId));
            return true;
        });
    }

    @Test
    void shouldAverageNeighborsWeighted() {
        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.of("weight")
        );

        var weightedConfig = ImmutableFastRPBaseConfig
            .builder()
            .from(DEFAULT_CONFIG)
            .relationshipWeightProperty("weight")
            .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
            .build();

        FastRP fastRP = new FastRP(
            graph,
            weightedConfig,
            defaultFeatureExtractors(graph),
            ProgressTracker.NULL_TRACKER
        );

        fastRP.initDegreePartition();
        fastRP.initPropertyVectors();
        fastRP.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = HugeObjectArray.newArray(float[].class, 3);
        fastRP.currentEmbedding(-1).copyTo(randomVectors, 3);
        fastRP.propagateEmbeddings();
        HugeObjectArray<float[]> embeddings = fastRP.embeddings();

        float[] expected = new float[DEFAULT_EMBEDDING_DIMENSION];
        for (int i = 0; i < DEFAULT_EMBEDDING_DIMENSION; i++) {
            expected[i] = (2.0f * randomVectors.get(1)[i] + randomVectors.get(2)[i]) / 2.0f;
        }
        l2Normalize(expected);

        assertThat(embeddings.get(0)).containsExactly(expected);
    }

    @Test
    void shouldDistributeValuesCorrectly() {
        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        var fastRP = new FastRP(
            graph,
            FastRPBaseConfig.builder()
                .embeddingDimension(512)
                .addIterationWeight(1.0D)
                .build(),
            List.of(),
            ProgressTracker.NULL_TRACKER
        );

        fastRP.initPropertyVectors();
        fastRP.initRandomVectors();
        HugeObjectArray<float[]> randomVectors = fastRP.currentEmbedding(-1);

        for (int i = 0; i < graph.nodeCount(); i++) {
            float[] embedding = randomVectors.get(i);
            int numZeros = 0;
            int numPositive = 0;
            for (int j = 0; j < 512; j++) {
                double embeddingValue = embedding[j];
                if (embeddingValue == 0) {
                    numZeros++;
                } else if (embeddingValue > 0) {
                    numPositive++;
                }
            }

            int numNegative = 512 - numZeros - numPositive;

            double p = 1D / 6D;
            int maxNumPositive = (int) ((p + 5D * Math.sqrt((p * (1 - p)) / 512D)) * 512D); // 1:30.000.000 chance of failing :P
            int minNumPositive = (int) ((p - 5D * Math.sqrt((p * (1 - p)) / 512D)) * 512D);

            assertThat(numPositive)
                .isGreaterThanOrEqualTo(minNumPositive)
                .isLessThanOrEqualTo(maxNumPositive);

            assertThat(numNegative)
                .isGreaterThanOrEqualTo(minNumPositive)
                .isLessThanOrEqualTo(maxNumPositive);
        }
    }

    @Test
    void shouldYieldEmptyEmbeddingForIsolatedNodes() {
        FastRP fastRP = new FastRP(
            scalarGraph,
            DEFAULT_CONFIG,
            List.of(),
            ProgressTracker.NULL_TRACKER
        );

        var embeddings = fastRP.embeddings();

        for (int i = 0; i < embeddings.size(); i++) {
            assertThat(embeddings.get(i)).containsOnly(0f);
        }
    }

    @Test
    void testMemoryEstimationWithoutIterationWeights() {
        var config = ImmutableFastRPBaseConfig
            .builder()
            .addIterationWeights(1.0D, 1.0D)
            .embeddingDimension(128)
            .build();

        var dimensions = ImmutableGraphDimensions.builder().nodeCount(100).build();

        var estimate = FastRP.memoryEstimation(config).estimate(dimensions, 1).memoryUsage();
        assertMemoryRange(estimate, 159_736);
    }

    @Test
    void testMemoryEstimationWithIterationWeights() {
        var config = ImmutableFastRPBaseConfig
            .builder()
            .embeddingDimension(128)
            .iterationWeights(List.of(1.0D, 2.0D))
            .build();

        var dimensions = ImmutableGraphDimensions.builder().nodeCount(100).build();

        var estimate = FastRP.memoryEstimation(config).estimate(dimensions, 1).memoryUsage();
        assertMemoryRange(estimate, 159_736);
    }

    @Test
    void shouldLogProgress() {
        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        var config = FastRPBaseConfig.builder()
            .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
            .propertyRatio(0.5)
            .featureProperties(List.of("f1", "f2", "f3"))
            .nodeSelfInfluence(0.6)
            .addIterationWeight(0.0D)
            .randomSeed(42L)
            .build();

        var factory = new FastRPFactory();

        var progressTask = factory.progressTask(graph, config);
        var log = Neo4jProxy.testLog();
        var progressTracker = new TaskProgressTracker(progressTask, log, 4, EmptyTaskRegistryFactory.INSTANCE);

        factory
            .build(graph, config, progressTracker)
            .compute();

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "FastRP :: Start",
                "FastRP :: Initialize random vectors :: Start",
                "FastRP :: Initialize random vectors 100%",
                "FastRP :: Initialize random vectors :: Finished",
                "FastRP :: Apply node self-influence :: Start",
                "FastRP :: Apply node self-influence 100%",
                "FastRP :: Apply node self-influence :: Finished",
                "FastRP :: Propagate embeddings :: Start",
                "FastRP :: Propagate embeddings :: Propagate embeddings task 1 of 1 :: Start",
                "FastRP :: Propagate embeddings :: Propagate embeddings task 1 of 1 100%",
                "FastRP :: Propagate embeddings :: Propagate embeddings task 1 of 1 :: Finished",
                "FastRP :: Propagate embeddings :: Finished",
                "FastRP :: Finished"
            );
    }

    @Nested
    @GdlExtension
    class MissingProperties {

        @GdlGraph
        private static final String DB_CYPHER =
            "CREATE" +
            "  (a:N { prop: 1 })" +
            ", (b:N)" +
            ", (c:NaNRelWeight)" +
            ", (d:NaNRelWeight)" +
            ", (a)-[:REL]->(b)" +
            ", (c)-[:REL]->(d)" +
            ", (d)-[:REL {weight: 1.0}]->(d)";

        @Inject
        GraphStore graphStore;

        @Inject
        IdFunction idFunction;

        @Test
        void shouldFailWhenNodePropertiesAreMissing() {
            Graph graph = graphStore.getGraph(NodeLabel.of("N"), RelationshipType.of("REL"), Optional.empty());
            FastRP fastRP = new FastRP(
                graph,
                FastRPBaseConfig.builder()
                    .embeddingDimension(64)
                    .addIterationWeights(1.0D, 1.0D, 1.0D, 1.0D)
                    .addFeatureProperty("prop")
                    .build(),
                FeatureExtraction.propertyExtractors(graph, List.of("prop")),
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(fastRP::initRandomVectors)
                .hasMessageContaining(
                    formatWithLocale(
                        "Node with ID `%s` has invalid feature property value `NaN` for property `prop`",
                        idFunction.of("b")
                    )
                );
        }

        @Test
        void shouldFailWhenRelationshipWeightIsMissing() {
            Graph graph = graphStore.getGraph(
                NodeLabel.of("NaNRelWeight"),
                RelationshipType.of("REL"),
                Optional.of("weight")
            );

            var weightedConfig = FastRPBaseConfig.builder()
                .relationshipWeightProperty("weight")
                .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
                .addIterationWeight(1.0D)
                .randomSeed(42L)
                .build();

            FastRP fastRP = new FastRP(
                graph,
                weightedConfig,
                List.of(),
                ProgressTracker.NULL_TRACKER
            );

            assertThatThrownBy(fastRP::compute)
                .hasMessageContaining(
                    formatWithLocale(
                        "Missing relationship property `weight` on relationship between nodes with ids `%d` and `%d`.",
                        idFunction.of("c"),
                        idFunction.of("d")
                    )
                );
        }
    }

    @Test
    void shouldBeDeterministicGivenSameOriginalIds() {
        long nodeCount = 1000;
        int embeddingDimension = 32;
        long degree = 4;

        var firstMappedToOriginal = HugeLongArray.newArray(nodeCount);
        firstMappedToOriginal.setAll(nodeId -> nodeId);
        var firstOriginalToMappedBuilder = HugeSparseLongArray.builder(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            firstOriginalToMappedBuilder.set(nodeId, nodeId);
        }
        var firstIdMap = new ArrayIdMap(
            firstMappedToOriginal,
            firstOriginalToMappedBuilder.build(),
            LabelInformationBuilders.singleLabel(NodeLabel.of("hello")).build(nodeCount, firstMappedToOriginal::get),
            nodeCount,
            nodeCount - 1
        );
        RelationshipsBuilder firstRelationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(firstIdMap)
            .orientation(Orientation.UNDIRECTED)
            .executorService(Pools.DEFAULT)
            .build();

        var secondMappedToOriginal = HugeLongArray.newArray(nodeCount);
        secondMappedToOriginal.setAll(nodeId -> nodeId);

        var gen = ShuffleUtil.createRandomDataGenerator(Optional.of(42L));
        ShuffleUtil.shuffleArray(secondMappedToOriginal, gen);
        var secondOriginalToMappedBuilder = HugeSparseLongArray.builder(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            secondOriginalToMappedBuilder.set(secondMappedToOriginal.get(nodeId), nodeId);
        }

        var secondIdMap = new ArrayIdMap(
            secondMappedToOriginal,
            secondOriginalToMappedBuilder.build(),
            LabelInformationBuilders.singleLabel(NodeLabel.of("hello")).build(nodeCount, secondMappedToOriginal::get),
            nodeCount,
            nodeCount - 1
        );
        RelationshipsBuilder secondRelationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(secondIdMap)
            .orientation(Orientation.UNDIRECTED)
            .executorService(Pools.DEFAULT)
            .build();

        var random = new SplittableRandom(42);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            for (int j = 0; j < degree; j++) {
                long target = random.nextLong(nodeCount);
                firstRelationshipsBuilder.add(nodeId, target);
                secondRelationshipsBuilder.add(nodeId, target);
            }
        }

        var firstRelationships = firstRelationshipsBuilder.build();
        var secondRelationships = secondRelationshipsBuilder.build();

        var firstGraph = GraphFactory.create(firstIdMap, firstRelationships);
        var secondGraph = GraphFactory.create(secondIdMap, secondRelationships);

        var config = ImmutableFastRPBaseConfig
            .builder()
            .embeddingDimension(embeddingDimension)
            .concurrency(1)
            .randomSeed(1337L)
            .build();

        var firstEmbeddings = new FastRP(
            firstGraph,
            config,
            List.of(),
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

        var secondEmbeddings = new FastRP(
            secondGraph,
            config,
            List.of(),
            ProgressTracker.NULL_TRACKER
        ).compute().embeddings();

        double cosineSum = 0;
        for (long originalNodeId = 0; originalNodeId < nodeCount; originalNodeId++) {
            var firstVector = firstEmbeddings.get(firstGraph.toMappedNodeId(originalNodeId));
            var secondVector = secondEmbeddings.get(secondGraph.toMappedNodeId(originalNodeId));
            double cosine = Intersections.cosine(firstVector, secondVector, secondVector.length);
            cosineSum += cosine;
        }
        assertThat(cosineSum / nodeCount).isCloseTo(1, Offset.offset(0.000001));
    }

    private HugeObjectArray<float[]> embeddings(Graph graph, List<String> properties) {
        var fastRPArray = new FastRP(
            graph,
            DEFAULT_CONFIG,
            FeatureExtraction.propertyExtractors(graph, properties),
            ProgressTracker.NULL_TRACKER
        );
        return fastRPArray.compute().embeddings();
    }

    private List<FeatureExtractor> defaultFeatureExtractors(Graph graph) {
        return FeatureExtraction.propertyExtractors(graph, DEFAULT_CONFIG.featureProperties());
    }

    private float[] takeLastElements(float[] input, int numLast) {
        var numDrop = input.length - numLast;
        var extractedResult = new float[numLast];
        System.arraycopy(input, numDrop, extractedResult, 0, numLast);
        return extractedResult;
    }
}
