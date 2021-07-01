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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.embeddings.graphsage.algo.ImmutableGraphSageTrainConfig;
import org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.embeddings.graphsage.GraphSageTestGraph;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@GdlExtension
class GraphSageEmbeddingsGeneratorTest {

    private static final int FEATURES_COUNT = 5;
    private static final int EMBEDDING_DIMENSION = 64;

    private static final String MODEL_NAME = "graphSageModel";

    @GdlGraph
    private static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    private Graph graph;

    @ParameterizedTest
    @EnumSource(Aggregator.AggregatorType.class)
    void makesEmbeddings(Aggregator.AggregatorType aggregatorType) {
        var config = ImmutableGraphSageTrainConfig.builder()
            .aggregator(aggregatorType)
            .embeddingDimension(EMBEDDING_DIMENSION)
            .featureProperties(Collections.nCopies(FEATURES_COUNT, "dummyProp"))
            .modelName(MODEL_NAME)
            .build();

        var features = GraphSageHelper.initializeSingleLabelFeatures(graph, config, AllocationTracker.empty());

        var trainModel = new GraphSageModelTrainer(config, Pools.DEFAULT, ProgressTracker.NULL_TRACKER);

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(graph, features);

        GraphSageEmbeddingsGenerator embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            result.layers(),
            config.batchSize(),
            config.concurrency(),
            config.isWeighted(),
            new SingleLabelFeatureFunction(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(graph, features);

        assertNotNull(embeddings);
        assertEquals(graph.nodeCount(), embeddings.size());

        LongStream.range(0, graph.nodeCount()).forEach(n -> assertEquals(EMBEDDING_DIMENSION, embeddings.get(n).length));
    }

    @ParameterizedTest
    @EnumSource(Aggregator.AggregatorType.class)
    void makesEmbeddingsFromMultiLabelModel(Aggregator.AggregatorType aggregatorType) {
        var config = ImmutableGraphSageTrainConfig.builder()
            .aggregator(aggregatorType)
            .modelName(MODEL_NAME)
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .embeddingDimension(EMBEDDING_DIMENSION)
            .projectedFeatureDimension(5)
            .build();

        var trainer = new MultiLabelGraphSageTrain(
            graph,
            config,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        var model = trainer.compute();

        var embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            model.data().layers(),
            config.batchSize(),
            config.concurrency(),
            config.isWeighted(),
            model.data().featureFunction(),
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER,
            AllocationTracker.empty()
        );

        var embeddings = embeddingsGenerator.makeEmbeddings(
            graph,
            GraphSageHelper.initializeMultiLabelFeatures(graph,
                GraphSageHelper.multiLabelFeatureExtractors(graph, config),
                AllocationTracker.empty()
            )
        );

        assertNotNull(embeddings);
        assertEquals(graph.nodeCount(), embeddings.size());

        LongStream.range(0, graph.nodeCount()).forEach(n -> assertEquals(EMBEDDING_DIMENSION, embeddings.get(n).length));
    }
}
