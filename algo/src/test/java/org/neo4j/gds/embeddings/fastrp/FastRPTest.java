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
import org.neo4j.gds.applications.algorithms.embeddings.NodeEmbeddingAlgorithms;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.collections.hsa.HugeSparseLongArray;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.ArrayIdMap;
import org.neo4j.gds.core.loading.LabelInformationBuilders;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.shuffle.ShuffleUtil;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.SplittableRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.l2Normalize;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@GdlExtension
class FastRPTest {

    private static final int DEFAULT_EMBEDDING_DIMENSION = 128;

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
        ", (e:Isolated {f1: -0.6, f2: 0.5, f3: 5.2})" +
        ", (a)-[:REL {weight: 2.0}]->(b)" +
        ", (b)-[:REL {weight: 1.0}]->(a)" +
        ", (a)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(a)" +
        ", (b)-[:REL {weight: 1.0}]->(c)" +
        ", (c)-[:REL {weight: 1.0}]->(b)";

    @Inject
    private Graph scalarGraph;

    @Inject
    private Graph arrayGraph;

    @Inject
    private GraphStore scalarGraphStore;

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
        var concurrency = 4;
        var minBatchSize = 10_000;

        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );
        FastRP fastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
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
        var concurrency = 4;
        var minBatchSize = 10_000;

        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );
        FastRP fastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
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
        var concurrency = 4;
        var minBatchSize = 10_000;
        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(0.0D),
            embeddingDimension,
            (int) (0.5 * embeddingDimension),
            Optional.empty(),
            0.0F,
            0.6
        );

        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );

        FastRP fastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
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
        var scale0 = parameters.nodeSelfInfluence().floatValue() / (float) Math.sqrt(2);
        var scale1 = parameters.nodeSelfInfluence().floatValue() / (float) Math.sqrt(2.4 * 2.4 + 0.5 * 0.5);
        var scale2 = parameters.nodeSelfInfluence().floatValue() / (float) Math.sqrt(3.0 * 3.0 + 0.5 * 0.5);
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
        var concurrency = 4;
        var minBatchSize = 10_000;

        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );
        FastRP fastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
        );

        fastRP.initPropertyVectors();

        // these asserted values were copied from the algorithm output. testing for stability.
        var expectedProp1 = new float[]{0.0f, -0.15309311f, 0.0f, 0.15309311f, 0.15309311f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.15309311f};
        var expectedProp2 = new float[]{-0.15309311f, 0.15309311f, 0.0f, 0.0f, -0.15309311f, 0.0f, -0.15309311f, 0.0f, -0.15309311f, 0.15309311f, 0.0f, -0.15309311f, 0.0f, -0.15309311f, 0.15309311f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, -0.15309311f, 0.0f, -0.15309311f, 0.0f, 0.0f, -0.15309311f, 0.15309311f, 0.15309311f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
        var expectedProp3 = new float[]{0.0f, -0.15309311f, -0.15309311f, 0.15309311f, 0.0f, 0.0f, -0.15309311f, -0.15309311f, 0.15309311f, -0.15309311f, 0.15309311f, -0.15309311f, 0.0f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, -0.15309311f, -0.15309311f, -0.15309311f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.15309311f, -0.15309311f, 0.0f, -0.15309311f, -0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.0f, 0.15309311f, 0.0f, 0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.15309311f, 0.0f, -0.15309311f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};

        assertArrayEquals(expectedProp1, fastRP.propertyVectors()[0]);
        assertArrayEquals(expectedProp2, fastRP.propertyVectors()[1]);
        assertArrayEquals(expectedProp3, fastRP.propertyVectors()[2]);

        fastRP.initRandomVectors();

        // these were obtained by computing a matrix product P * V where
        //    P is the propertyDimension x inputDimension matrix with expected propertyVectors as columns
        //    V is the inputDimension x nodeCount matrix of property values in the graph
        var initialPropComponentOfNodeVector1 = new float[]{-0.19902104f, -0.076546565f, -0.21433036f, 0.2755676f, -0.1377838f, 0.0f, -0.47458863f, -0.21433036f, 0.015309319f, -0.015309319f, 0.21433036f, -0.41335142f, 0.0f, -0.19902104f, -0.015309319f, 0.061237246f, 0.19902104f, 0.0f, -0.2755676f, -0.015309319f, -0.21433036f, 0.19902104f, -0.061237246f, 0.21433036f, 0.0f, 0.061237246f, 0.21433036f, -0.19902104f, 0.0f, 0.015309319f, -0.21433036f, 0.0f, -0.35211414f, -0.015309319f, 0.19902104f, 0.19902104f, 0.0f, 0.0f, 0.0f, 0.0f, 0.015309319f, -0.061237246f, 0.21433036f, 0.0f, 0.21433036f, 0.2602583f, 0.061237246f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.41335142f, 0.0f, -0.21433036f, -0.061237246f, 0.0f, 0.0f, 0.0f, -0.061237246f, 0.0f, 0.061237246f};
        var initialPropComponentOfNodeVector2 = new float[]{-0.07654656f, -0.5205166f, -0.2755676f, 0.5970631f, 0.24494898f, 0.0f, -0.6736097f, -0.2755676f, 0.19902104f, -0.19902104f, 0.2755676f, -0.35211414f, 0.0f, -0.07654656f, -0.19902104f, 0.32149553f, 0.07654656f, 0.0f, -0.5970631f, -0.19902104f, -0.2755676f, 0.07654656f, -0.32149553f, 0.2755676f, 0.0f, 0.32149553f, 0.2755676f, -0.07654656f, 0.0f, 0.19902104f, -0.2755676f, 0.0f, -0.030618608f, -0.19902104f, 0.07654656f, 0.07654656f, 0.0f, 0.0f, 0.0f, 0.0f, 0.19902104f, -0.32149553f, 0.2755676f, 0.0f, 0.2755676f, 0.39804208f, 0.32149553f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.35211414f, 0.0f, -0.2755676f, -0.32149553f, 0.0f, 0.0f, 0.0f, -0.32149553f, 0.0f, 0.32149553f};
        var initialPropComponentOfNodeVector3 = new float[]{-0.12247449f, -0.2602583f, -0.42866072f, 0.38273278f, -0.16840243f, 0.0f, -0.5052073f, -0.42866072f, 0.30618623f, -0.30618623f, 0.42866072f, -0.5511352f, 0.0f, -0.12247449f, -0.30618623f, -0.045927934f, 0.12247449f, 0.0f, -0.38273278f, -0.30618623f, -0.42866072f, 0.12247449f, 0.045927934f, 0.42866072f, 0.0f, -0.045927934f, 0.42866072f, -0.12247449f, 0.0f, 0.30618623f, -0.42866072f, 0.0f, -0.5970632f, -0.30618623f, 0.12247449f, 0.12247449f, 0.0f, 0.0f, 0.0f, 0.0f, 0.30618623f, 0.045927934f, 0.42866072f, 0.0f, 0.42866072f, 0.07654656f, -0.045927934f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.5511352f, 0.0f, -0.42866072f, 0.045927934f, 0.0f, 0.0f, 0.0f, 0.045927934f, 0.0f, -0.045927934f};

        assertThat(initialPropComponentOfNodeVector1)
            .contains(
                takeLastElements(fastRP.currentEmbedding(-1).get(0), parameters.propertyDimension()),
                Offset.offset(1e-6f)
            );
        assertThat(initialPropComponentOfNodeVector2)
            .contains(
                takeLastElements(fastRP.currentEmbedding(-1).get(1), parameters.propertyDimension()),
                Offset.offset(1e-6f)
            );
        assertThat(initialPropComponentOfNodeVector3)
            .contains(
                takeLastElements(fastRP.currentEmbedding(-1).get(2), parameters.propertyDimension()),
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
        var minBatchSize = 1;

        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );

        FastRP concurrentFastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(4),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
        );

        concurrentFastRP.compute();
        HugeObjectArray<float[]> concurrentEmbeddings = concurrentFastRP.embeddings();

        FastRP sequentialFastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(1),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
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
        var concurrency = 4;
        var minBatchSize = 10_000;

        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.of("weight"),
            0.0F,
            0
        );

        FastRP fastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, parameters.featureProperties()),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
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
        var concurrency = 4;
        var minBatchSize = 10_000;

        var parameters = new FastRPParameters(
            List.of(),
            List.of(1.0D),
            512,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );

        var fastRP = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            List.of(),
            ProgressTracker.NULL_TRACKER,
            Optional.empty(),
            TerminationFlag.RUNNING_TRUE
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
        var concurrency = 4;
        var minBatchSize = 10_000;
        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );
        FastRP fastRP = new FastRP(
            scalarGraph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            List.of(),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
        );

        var embeddings = fastRP.embeddings();

        for (int i = 0; i < embeddings.size(); i++) {
            assertThat(embeddings.get(i)).containsOnly(0f);
        }
    }

    @Test
    void shouldLogProgress() {
        var log = new GdsTestLog();
        var progressTrackerCreator = new ProgressTrackerCreator(log, RequestScopedDependencies.builder()
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .build());
        var nodeEmbeddingAlgorithms = new NodeEmbeddingAlgorithms(null, progressTrackerCreator, TerminationFlag.RUNNING_TRUE);

        var graph = scalarGraphStore.getGraph(
            List.of(NodeLabel.of("Node1"), NodeLabel.of("Node2")),
            List.of(RelationshipType.of("REL")),
            Optional.empty()
        );
        var configuration = FastRPBaseConfigImpl.builder()
            .concurrency(4)
            .embeddingDimension(DEFAULT_EMBEDDING_DIMENSION)
            .featureProperties(List.of("f1", "f2", "f3"))
            .iterationWeights(List.of(0.0D))
            .nodeSelfInfluence(0.6)
            .normalizationStrength(0.0F)
            .randomSeed(42L)
            .build();
        nodeEmbeddingAlgorithms.fastRP(graph, configuration);

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
        private GraphStore graphStore;

        @Inject
        private IdFunction idFunction;

        @Test
        void shouldFailWhenNodePropertiesAreMissing() {
            Graph graph = graphStore.getGraph(NodeLabel.of("N"), RelationshipType.of("REL"), Optional.empty());
            var concurrency = 4;
            var minBatchSize = 10_000;
            var parameters = new FastRPParameters(
                List.of("prop"),
                List.of(1.0D, 1.0D, 1.0D, 1.0D),
                64,
                0,
                Optional.empty(),
                0.0F,
                0
            );
            FastRP fastRP = new FastRP(
                graph,
                parameters,
                new Concurrency(concurrency),
                minBatchSize,
                FeatureExtraction.propertyExtractors(graph, List.of("prop")),
                ProgressTracker.NULL_TRACKER,
                Optional.empty(),
                TerminationFlag.RUNNING_TRUE
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
            var concurrency = 4;
            var minBatchSize = 10_000;

            var parameters = new FastRPParameters(
                List.of(),
                List.of(1.0D),
                DEFAULT_EMBEDDING_DIMENSION,
                0,
                Optional.of("weight"),
                0.0F,
                0
            );

            FastRP fastRP = new FastRP(
                graph,
                parameters,
                new Concurrency(concurrency),
                minBatchSize,
                List.of(),
                ProgressTracker.NULL_TRACKER,
                Optional.of(42L),
                TerminationFlag.RUNNING_TRUE
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
            .relationshipType(RelationshipType.of("REL"))
            .orientation(Orientation.UNDIRECTED)
            .executorService(DefaultPool.INSTANCE)
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
            .relationshipType(RelationshipType.of("REL"))
            .orientation(Orientation.UNDIRECTED)
            .executorService(DefaultPool.INSTANCE)
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

        var concurrency = 1;
        var minBatchSize = 10_000;
        var parameters = new FastRPParameters(
            List.of(),
            List.of(1.0D),
            embeddingDimension,
            0,
            Optional.empty(),
            0.0F,
            0
        );

        var firstEmbeddings = new FastRP(
            firstGraph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            List.of(),
            ProgressTracker.NULL_TRACKER,
            Optional.of(1337L),
            TerminationFlag.RUNNING_TRUE
        ).compute().embeddings();

        var secondEmbeddings = new FastRP(
            secondGraph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            List.of(),
            ProgressTracker.NULL_TRACKER,
            Optional.of(1337L),
            TerminationFlag.RUNNING_TRUE
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
        var concurrency = 4;
        var minBatchSize = 10_000;
        var parameters = new FastRPParameters(
            List.of("f1", "f2", "f3"),
            List.of(1.0D),
            DEFAULT_EMBEDDING_DIMENSION,
            (int) (0.5 * DEFAULT_EMBEDDING_DIMENSION),
            Optional.empty(),
            0.0F,
            0
        );
        var fastRPArray = new FastRP(
            graph,
            parameters,
            new Concurrency(concurrency),
            minBatchSize,
            FeatureExtraction.propertyExtractors(graph, properties),
            ProgressTracker.NULL_TRACKER,
            Optional.of(42L),
            TerminationFlag.RUNNING_TRUE
        );
        return fastRPArray.compute().embeddings();
    }

    private float[] takeLastElements(float[] input, int numLast) {
        var numDrop = input.length - numLast;
        var extractedResult = new float[numLast];
        System.arraycopy(input, numDrop, extractedResult, 0, numLast);
        return extractedResult;
    }
}
