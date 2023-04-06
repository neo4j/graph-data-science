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
package org.neo4j.gds.ml.pipeline.nodePipeline.regression;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.pipeline.PipelineTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult.NodeRegressionTrainPipelineResult;

public class NodeRegressionTrainAlgorithm extends PipelineTrainAlgorithm<
    NodeRegressionTrainResult,
    NodeRegressionTrainPipelineResult,
    NodeRegressionPipelineTrainConfig,
    NodeFeatureStep> {

    NodeRegressionTrainAlgorithm(
        NodeRegressionTrain pipelineTrainer,
        NodeRegressionTrainingPipeline pipeline,
        GraphStore graphStore,
        NodeRegressionPipelineTrainConfig config,
        ProgressTracker progressTracker,
        String gdsVersion
    ) {
        super(pipelineTrainer, pipeline, new NodeRegressionToModelConverter(pipeline, config, gdsVersion), graphStore, config, progressTracker);
    }

}
