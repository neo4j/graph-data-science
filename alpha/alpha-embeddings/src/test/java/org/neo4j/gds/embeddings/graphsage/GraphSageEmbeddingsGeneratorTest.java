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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

import java.util.Collections;
import java.util.Random;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GraphSageEmbeddingsGeneratorTest {

    private final int FEATURES_COUNT = 5;
    private final int EMBEDDING_DIMENSION = 64;

    private final String MODEL_NAME = "graphSageModel";

    private Graph graph;
    private HugeObjectArray<double[]> features;
    private ImmutableGraphSageTrainConfig.Builder configBuilder;

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
            .allocationTracker(AllocationTracker.empty())
            .build();
        graph = randomGraphGenerator.generate();

        long nodeCount = graph.nodeCount();
        features = HugeObjectArray.newArray(double[].class, nodeCount, AllocationTracker.empty());

        Random random = new Random();
        LongStream.range(0, nodeCount).forEach(n -> features.set(n, random.doubles(FEATURES_COUNT).toArray()));

        configBuilder = ImmutableGraphSageTrainConfig.builder()
            .nodePropertyNames(Collections.nCopies(FEATURES_COUNT, "dummyNodeProperty"))
            .embeddingDimension(EMBEDDING_DIMENSION);
    }

    @ParameterizedTest
    @EnumSource(Aggregator.AggregatorType.class)
    void makesEmbeddings(Aggregator.AggregatorType aggregatorType) {
        var config = configBuilder
            .aggregator(aggregatorType)
            .modelName(MODEL_NAME)
            .build();

        var trainModel = new GraphSageModelTrainer(config, ProgressLogger.NULL_LOGGER);

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(graph, features);

        GraphSageEmbeddingsGenerator embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            result.layers(),
            config.batchSize(),
            config.concurrency(),
            AllocationTracker.empty()
        );

        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(graph, features);

        assertNotNull(embeddings);
        assertEquals(graph.nodeCount(), embeddings.size());

        LongStream.range(0, graph.nodeCount()).forEach(n -> assertEquals(EMBEDDING_DIMENSION, embeddings.get(n).length));
    }

}
