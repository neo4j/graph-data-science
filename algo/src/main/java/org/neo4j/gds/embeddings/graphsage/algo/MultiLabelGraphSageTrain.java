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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.GraphSageHelper;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.MultiLabelFeatureFunction;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeMultiLabelFeatures;
import static org.neo4j.gds.embeddings.graphsage.LayerFactory.generateWeights;

public class MultiLabelGraphSageTrain extends GraphSageTrain {

    private static final double WEIGHT_BOUND = 1.0D;

    private final Graph graph;
    private final GraphSageTrainConfig config;
    private final ExecutorService executor;

    private final String gdsVersion;

    public MultiLabelGraphSageTrain(
        Graph graph,
        GraphSageTrainConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker,
        String gdsVersion
    ) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
        this.executor = executor;
        this.gdsVersion = gdsVersion;
    }

    @Override
    public Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> compute() {
        progressTracker.beginSubTask("GraphSageTrain");

        var multiLabelFeatureExtractors = GraphSageHelper.multiLabelFeatureExtractors(graph, config);
        var weightsByLabel = MultiLabelGraphSageTrain.makeWeightsByLabel(config, multiLabelFeatureExtractors);
        var projectedFeatureDimension = config.projectedFeatureDimension().orElseThrow();
        var multiLabelFeatureFunction = new MultiLabelFeatureFunction(weightsByLabel, projectedFeatureDimension);
        var trainer = new GraphSageModelTrainer(
            config,
            executor,
            progressTracker,
            multiLabelFeatureFunction,
            multiLabelFeatureFunction.weightsByLabel().values()
        );

        var trainResult = trainer.train(
            graph,
            initializeMultiLabelFeatures(graph, multiLabelFeatureExtractors)
        );

        progressTracker.endSubTask("GraphSageTrain");

        return Model.of(
            gdsVersion,
            GraphSage.MODEL_TYPE,
            graph.schema(),
            ModelData.of(trainResult.layers(), multiLabelFeatureFunction),
            config,
            trainResult.metrics()
        );
    }

    private static Map<NodeLabel, Weights<Matrix>> makeWeightsByLabel(
        GraphSageTrainConfig config,
        MultiLabelFeatureExtractors multiLabelFeatureExtractors
    ) {
        return multiLabelFeatureExtractors.featureCountPerLabel()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                //TODO: how should we initialize the values in the matrix?
                e -> generateWeights(
                    config.projectedFeatureDimension().orElseThrow(),
                    e.getValue(),
                    WEIGHT_BOUND,
                    config.randomSeed().orElseGet(() -> ThreadLocalRandom.current().nextLong())
                )
            ));
    }
}
