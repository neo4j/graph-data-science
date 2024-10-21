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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.ml.pipeline.ExecutableNodePropertyStep;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkFeatureStep;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;

public final class PipelineInfoResultTransformer {


    private PipelineInfoResultTransformer() {}

    public static PipelineInfoResult create(PipelineName pipelineName, LinkPredictionTrainingPipeline pipeline) {
        return create(pipelineName.value, pipeline);
    }

    /**
     * @deprecated migrate to the other one
     */
    @Deprecated
    public static PipelineInfoResult create(String pipelineName, LinkPredictionTrainingPipeline pipeline) {
        var nodePropertySteps = pipeline
            .nodePropertySteps()
            .stream()
            .map(ExecutableNodePropertyStep::toMap)
            .toList();

        var featureSteps = pipeline.featureSteps().stream().map(LinkFeatureStep::toMap).toList();

        return new PipelineInfoResult(
            pipelineName,
            nodePropertySteps,
            featureSteps,
            pipeline.splitConfig().toMap(),
            pipeline.autoTuningConfig().toMap(),
            TrainingPipeline.toMapParameterSpace(pipeline.trainingParameterSpace())
        );
    }
}
