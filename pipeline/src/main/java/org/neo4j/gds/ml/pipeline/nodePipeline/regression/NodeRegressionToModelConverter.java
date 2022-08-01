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

import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.ml.pipeline.ResultToModelConverter;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult.NodeRegressionTrainPipelineResult;

public class NodeRegressionToModelConverter implements ResultToModelConverter<NodeRegressionTrainPipelineResult, NodeRegressionTrainResult> {
    private final NodeRegressionTrainingPipeline pipeline;
    private final NodeRegressionPipelineTrainConfig config;

    public NodeRegressionToModelConverter(
        NodeRegressionTrainingPipeline pipeline,
        NodeRegressionPipelineTrainConfig config
    ) {
        this.pipeline = pipeline;
        this.config = config;
    }

    @Override
    public NodeRegressionTrainPipelineResult toModel(
        NodeRegressionTrainResult trainResult,
        GraphSchema originalSchema
    ) {
        var catalogModel = Model.of(
            NodeRegressionTrainingPipeline.MODEL_TYPE,
            originalSchema,
            trainResult.regressor().data(),
            config,
            NodeRegressionPipelineModelInfo.of(
                trainResult.trainingStatistics().winningModelTestMetrics(),
                trainResult.trainingStatistics().winningModelOuterTrainMetrics(),
                trainResult.trainingStatistics().bestCandidate(),
                NodePropertyPredictPipeline.from(pipeline)
            )
        );

        return ImmutableNodeRegressionTrainPipelineResult.of(catalogModel, trainResult.trainingStatistics());
    }
}
