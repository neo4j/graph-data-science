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
package org.neo4j.gds.ml.pipeline.nodePipeline.classification.train;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.pipeline.PipelineTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.PipelineTrainer;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainResult.NodeClassificationModelResult;

public class NodeClassificationTrainAlgorithm extends PipelineTrainAlgorithm<
    NodeClassificationTrainResult,
    NodeClassificationModelResult,
    NodeClassificationPipelineTrainConfig,
    NodeFeatureStep> {

    NodeClassificationTrainAlgorithm(
        PipelineTrainer<NodeClassificationTrainResult> pipelineTrainer,
        TrainingPipeline<NodeFeatureStep> pipeline,
        GraphStore graphStore,
        NodeClassificationPipelineTrainConfig config,
        ProgressTracker progressTracker
    ) {
        super(pipelineTrainer, pipeline, graphStore, config, progressTracker);
    }

    @Override
    protected NodeClassificationModelResult transformResult(
        NodeClassificationTrainResult inputResult,
        NodeClassificationPipelineTrainConfig config,
        GraphSchema originalSchema
    ) {
        var catalogModel = Model.of(
            config.username(),
            config.modelName(),
            NodeClassificationTrainingPipeline.MODEL_TYPE,
            originalSchema,
            inputResult.classifier().data(),
            config,
            NodeClassificationPipelineModelInfo.of(
                inputResult.trainingStatistics().winningModelTestMetrics(),
                inputResult.trainingStatistics().winningModelOuterTrainMetrics(),
                inputResult.trainingStatistics().bestCandidate(),
                NodePropertyPredictPipeline.from(pipeline),
                inputResult.classIdMap().originalIdsList()
            )
        );

        return ImmutableNodeClassificationModelResult.of(catalogModel, inputResult.trainingStatistics());
    }
}
