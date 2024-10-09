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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.CSRGraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.algo.GraphSageTrainConfigImpl;
import org.neo4j.gds.embeddings.graphsage.algo.MultiLabelGraphSageTrain;
import org.neo4j.gds.embeddings.graphsage.algo.SingleLabelGraphSageTrain;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.gds.TestGdsVersion.testGdsVersion;

@GdlExtension
class GraphSageEmbeddingsGeneratorTest {

    private static final int FEATURES_COUNT = 5;
    private static final int EMBEDDING_DIMENSION = 64;

    private static final String MODEL_NAME = "graphSageModel";

    @GdlGraph(graphNamePrefix = "weighted")
    private static final String GDL = GraphSageTestGraph.GDL;

    @Inject
    private Graph weightedGraph;

    @ParameterizedTest
    @EnumSource(AggregatorType.class)
    void makesEmbeddings(AggregatorType aggregatorType) {
        var config = GraphSageTrainConfigImpl.builder()
            .aggregator(aggregatorType)
            .embeddingDimension(EMBEDDING_DIMENSION)
            .featureProperties(Collections.nCopies(FEATURES_COUNT, "dummyProp"))
            .modelName(MODEL_NAME)
            .modelUser("")
            .relationshipWeightProperty("times")
            .build();

        var parameters = TrainConfigTransformer.toParameters(config);

        var features = GraphSageHelper.initializeSingleLabelFeatures(weightedGraph, parameters.featureProperties());
        var featureDimension = FeatureExtraction.featureCount(weightedGraph, parameters.featureProperties());
        var trainModel = new GraphSageModelTrainer(parameters, featureDimension, DefaultPool.INSTANCE, ProgressTracker.NULL_TRACKER, TerminationFlag.RUNNING_TRUE);

        GraphSageModelTrainer.ModelTrainResult result = trainModel.train(weightedGraph, features);



        GraphSageEmbeddingsGenerator embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            result.layers(),
            parameters.batchSize(),
            parameters.concurrency(),
            new SingleLabelFeatureFunction(),
            parameters.randomSeed(),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        HugeObjectArray<double[]> embeddings = embeddingsGenerator.makeEmbeddings(weightedGraph, features);

        assertNotNull(embeddings);
        assertEquals(weightedGraph.nodeCount(), embeddings.size());

        LongStream.range(0, weightedGraph.nodeCount()).forEach(n -> assertEquals(EMBEDDING_DIMENSION, embeddings.get(n).length));
    }

    @ParameterizedTest
    @EnumSource(AggregatorType.class)
    void makesEmbeddingsFromMultiLabelModel(AggregatorType aggregatorType) {
        var config = GraphSageTrainConfigImpl.builder()
            .aggregator(aggregatorType)
            .modelName(MODEL_NAME)
            .modelUser("")
            .featureProperties(List.of("numEmployees", "numIngredients", "rating", "numPurchases"))
            .embeddingDimension(EMBEDDING_DIMENSION)
            .projectedFeatureDimension(5)
            .relationshipWeightProperty("times")
            .build();

        var trainer = new MultiLabelGraphSageTrain(
            weightedGraph,
            TrainConfigTransformer.toParameters(config),
            5,
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            testGdsVersion,
            config
        );

        var model = trainer.compute();

        var embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            model.data().layers(),
            config.batchSize(),
            config.concurrency(),
            model.data().featureFunction(),
            model.trainConfig().randomSeed(),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var embeddings = embeddingsGenerator.makeEmbeddings(
            weightedGraph,
            GraphSageHelper.initializeMultiLabelFeatures(
                weightedGraph,
                GraphSageHelper.multiLabelFeatureExtractors(weightedGraph, config.featureProperties())
            )
        );

        assertNotNull(embeddings);
        assertEquals(weightedGraph.nodeCount(), embeddings.size());

        LongStream.range(0, weightedGraph.nodeCount()).forEach(n -> assertEquals(EMBEDDING_DIMENSION, embeddings.get(n).length));
    }

    @Test
    void embeddingsForNodeFilteredGraph() {
        GdlFactory factory = GdlFactory.of(
            "(a:Ignore {age: 1}), (b:N {age: 42}), (c:N {age: 13}), (d:N {age: 14}), " +
            "(a)-->(d), " +
            "(b)-->(c), " +
            "(c)-->(d), " +
            "(d)-->(b)"
        );
        CSRGraphStore graphStore = factory.build();
        Graph filteredGraph = graphStore.getGraph("N", RelationshipType.ALL_RELATIONSHIPS.name, Optional.empty());

        var config = GraphSageTrainConfigImpl.builder()
            .aggregator(AggregatorType.MEAN)
            .modelName("DUMMY")
            .modelUser("")
            .sampleSizes(List.of(2))
            .featureProperties(List.of("age"))
            .embeddingDimension(3)
            .build();

        var trainer = new SingleLabelGraphSageTrain(
            filteredGraph,
            TrainConfigTransformer.toParameters(config),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE,
            testGdsVersion,
            config
        );

        var model = trainer.compute();

        var embeddingsGenerator = new GraphSageEmbeddingsGenerator(
            model.data().layers(),
            config.batchSize(),
            config.concurrency(),
            model.data().featureFunction(),
            model.trainConfig().randomSeed(),
            DefaultPool.INSTANCE,
            ProgressTracker.NULL_TRACKER,
            TerminationFlag.RUNNING_TRUE
        );

        var embeddings = embeddingsGenerator.makeEmbeddings(
            filteredGraph,
            GraphSageHelper.initializeSingleLabelFeatures(filteredGraph, config.featureProperties())
        );

        assertThat(embeddings)
            .isNotNull()
            .matches(i -> i.size() == filteredGraph.nodeCount());
    }
}
