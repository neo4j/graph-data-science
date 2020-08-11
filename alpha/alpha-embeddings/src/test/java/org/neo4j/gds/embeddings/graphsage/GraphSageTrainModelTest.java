/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageBaseConfig;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageStreamConfig;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphSageTrainModelTest {

    private final int FEATURES_COUNT = 5;
    private final int EMBEDDING_SIZE = 64;

    private Graph graph;
    private HugeObjectArray<double[]> features;
    private ImmutableGraphSageStreamConfig.Builder configBuilder;

    @BeforeEach
    void setUp() {
        RandomGraphGenerator randomGraphGenerator = RandomGraphGenerator.builder()
            .nodeCount(20)
            .averageDegree(3)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(123L)
            .aggregation(Aggregation.SINGLE)
            .orientation(Orientation.UNDIRECTED)
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.NO)
            .allocationTracker(AllocationTracker.EMPTY)
            .build();
        graph = randomGraphGenerator.generate();

        long nodeCount = graph.nodeCount();
        features = HugeObjectArray.newArray(double[].class, nodeCount, AllocationTracker.EMPTY);

        Random random = new Random();
        LongStream.range(0, nodeCount).forEach(n -> features.set(n, random.doubles(FEATURES_COUNT).toArray()));
        configBuilder = ImmutableGraphSageStreamConfig.builder()
            .nodePropertyNames(Collections.nCopies(FEATURES_COUNT, "dummyNodeProperty"))
            .embeddingSize(EMBEDDING_SIZE);
    }

    @Test
    void trainsWithMeanAggregator() {
        GraphSageBaseConfig config = configBuilder
            .aggregator(Aggregator.AggregatorType.MEAN)
            .build();

        var trainModel = new GraphSageTrainModel(config, new TestLog());

        GraphSageTrainModel.ModelTrainResult result = trainModel.train(graph, features);

        Layer[] layers = result.layers();
        assertEquals(2, layers.length);
        Layer first = layers[0];
        List<Weights<? extends Tensor<?>>> firstWeights = first.weights();
        assertEquals(1, firstWeights.size());

        // First layer is (embeddingSize x features.length)
        assertArrayEquals(new int[]{EMBEDDING_SIZE, FEATURES_COUNT}, firstWeights.get(0).dimensions());
        Layer second = layers[1];
        List<Weights<? extends Tensor<?>>> secondWeights = second.weights();
        assertEquals(1, secondWeights.size());

        // Second layer weights (embeddingSize x embeddingSize)
        assertArrayEquals(new int[]{EMBEDDING_SIZE, EMBEDDING_SIZE}, secondWeights.get(0).dimensions());
    }

    @Test
    void trainsWithPoolAggregator() {
        GraphSageBaseConfig config = configBuilder
            .aggregator(Aggregator.AggregatorType.POOL)
            .build();

        var trainModel = new GraphSageTrainModel(config, new TestLog());

        GraphSageTrainModel.ModelTrainResult result = trainModel.train(graph, features);
        Layer[] layers = result.layers();
        assertEquals(2, layers.length);

        Layer first = layers[0];
        List<Weights<? extends Tensor<?>>> firstWeights = first.weights();
        assertEquals(4, firstWeights.size());

        var firstLayerPoolWeights = firstWeights.get(0).dimensions();
        var firstLayerSelfWeights = firstWeights.get(1).dimensions();
        var firstLayerNeighborsWeights = firstWeights.get(2).dimensions();
        var firstLayerBias = firstWeights.get(3).dimensions();
        assertArrayEquals(new int[]{EMBEDDING_SIZE, FEATURES_COUNT}, firstLayerPoolWeights);
        assertArrayEquals(new int[]{EMBEDDING_SIZE, FEATURES_COUNT}, firstLayerSelfWeights);
        assertArrayEquals(new int[]{EMBEDDING_SIZE, EMBEDDING_SIZE}, firstLayerNeighborsWeights);
        assertArrayEquals(new int[]{EMBEDDING_SIZE}, firstLayerBias);

        Layer second = layers[1];
        List<Weights<? extends Tensor<?>>> secondWeights = second.weights();
        assertEquals(4, secondWeights.size());

        var secondLayerPoolWeights = secondWeights.get(0).dimensions();
        var secondLayerSelfWeights = secondWeights.get(1).dimensions();
        var secondLayerNeighborsWeights = secondWeights.get(2).dimensions();
        var secondLayerBias = secondWeights.get(3).dimensions();
        assertArrayEquals(new int[]{EMBEDDING_SIZE, EMBEDDING_SIZE}, secondLayerPoolWeights);
        assertArrayEquals(new int[]{EMBEDDING_SIZE, EMBEDDING_SIZE}, secondLayerSelfWeights);
        assertArrayEquals(new int[]{EMBEDDING_SIZE, EMBEDDING_SIZE}, secondLayerNeighborsWeights);
        assertArrayEquals(new int[]{EMBEDDING_SIZE}, secondLayerBias);
    }

}
