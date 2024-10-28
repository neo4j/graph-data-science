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
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyTrainingPipeline;

import java.util.stream.Collectors;

public final class NodePipelineInfoResultTransformer {

    private NodePipelineInfoResultTransformer() {}

    static NodePipelineInfoResult create(PipelineName pipelineName, NodePropertyTrainingPipeline pipeline) {
        return create(pipelineName.value, pipeline);
    }

    /**
     * @deprecated This can go soon
     */
    @Deprecated
    public static NodePipelineInfoResult create(String pipelineName, NodePropertyTrainingPipeline pipeline) {
        var nodePropertySteps = pipeline
            .nodePropertySteps()
            .stream()
            .map(ExecutableNodePropertyStep::toMap)
            .collect(Collectors.toList());

        return new NodePipelineInfoResult(
            pipelineName,
            nodePropertySteps,
            pipeline.featureProperties(),
            pipeline.splitConfig().toMap(),
            pipeline.autoTuningConfig().toMap(),
            TrainingPipeline.toMapParameterSpace(pipeline.trainingParameterSpace())
        );
    }
}
