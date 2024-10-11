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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.ModelData;
import org.neo4j.gds.embeddings.graphsage.SingleLabelFeatureFunction;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;

import static org.neo4j.gds.embeddings.graphsage.GraphSageHelper.initializeSingleLabelFeatures;

public class SingleLabelGraphSageTrain extends GraphSageTrain {

    private final Graph graph;
    private final ExecutorService executor;

    private final String gdsVersion;
    private final GraphSageTrainParameters parameters;
    @Deprecated  private final GraphSageTrainConfig config;

    public SingleLabelGraphSageTrain(
        Graph graph,
        GraphSageTrainParameters parameters,
        ExecutorService executor,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        String gdsVersion,
        GraphSageTrainConfig config // TODO: Last trace of UI config in here--Once we attach Parameters to Models we can lose this too
    ) {
        super(progressTracker, terminationFlag);
        this.graph = graph;
        this.parameters = parameters;
        this.executor = executor;
        this.gdsVersion = gdsVersion;
        this.config = config;
    }

    @Override
    public Model<ModelData, GraphSageTrainConfig, GraphSageModelTrainer.GraphSageTrainMetrics> compute() {
        progressTracker.beginSubTask("GraphSageTrain");

        var featureDimension = FeatureExtraction.featureCount(graph, parameters.featureProperties());
        var graphSageModel = new GraphSageModelTrainer(
            parameters,
            featureDimension,
            executor,
            progressTracker,
            terminationFlag
        );

        GraphSageModelTrainer.ModelTrainResult trainResult = graphSageModel.train(
            graph,
            initializeSingleLabelFeatures(graph, parameters.featureProperties())
        );

        progressTracker.endSubTask("GraphSageTrain");

        return Model.of(
            gdsVersion,
            GraphSage.MODEL_TYPE,
            graph.schema(),
            ModelData.of(trainResult.layers(), new SingleLabelFeatureFunction()),
            config,
            trainResult.metrics()
        );
    }
}
