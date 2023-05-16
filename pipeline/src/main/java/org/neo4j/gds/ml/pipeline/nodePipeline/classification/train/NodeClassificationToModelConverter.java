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

import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.ml.pipeline.ResultToModelConverter;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

public class NodeClassificationToModelConverter implements ResultToModelConverter<NodeClassificationModelResult, NodeClassificationTrainResult> {
    private final NodeClassificationTrainingPipeline pipeline;
    private final NodeClassificationPipelineTrainConfig config;

    private final String gdsVersion;

    public NodeClassificationToModelConverter(
        NodeClassificationTrainingPipeline pipeline,
        NodeClassificationPipelineTrainConfig config,
        String gdsVersion
    ) {
        this.pipeline = pipeline;
        this.config = config;
        this.gdsVersion = gdsVersion;
    }

    @Override
    public NodeClassificationModelResult toModel(
        NodeClassificationTrainResult result, GraphSchema originalSchema
    ) {
        var catalogModel = Model.of(
            gdsVersion,
            NodeClassificationTrainingPipeline.MODEL_TYPE,
            originalSchema,
            result.classifier().data(),
            config,
            NodeClassificationPipelineModelInfo.of(
                result.trainingStatistics().winningModelTestMetrics(),
                result.trainingStatistics().winningModelOuterTrainMetrics(),
                result.trainingStatistics().bestCandidate(),
                NodePropertyPredictPipeline.from(pipeline),
                result.classIdMap().originalIdsList()
            )
        );

        return ImmutableNodeClassificationModelResult.of(catalogModel, result.trainingStatistics());
    }
}
