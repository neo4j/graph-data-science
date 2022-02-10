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
package org.neo4j.gds.ml.nodemodels.pipeline;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.StringIdentifierValidations;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.pipeline.PipelineCreateConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipeline;

import static org.neo4j.gds.ml.nodemodels.pipeline.NodeClassificationPipelineCompanion.PIPELINE_MODEL_TYPE;

public class NodeClassificationPipelineCreate {

    public static PipelineInfoResult create(
        ModelCatalog modelCatalog,
        String username,
        String pipelineName
    ) {
        StringIdentifierValidations.validateNoWhiteCharacter(pipelineName, "pipelineName");

        var pipeline = new NodeClassificationPipeline();

        modelCatalog.set(fromTrainingPipelineToModel(
            username,
            pipelineName,
            pipeline
        ));

        return new PipelineInfoResult(pipelineName, pipeline);
    }

    @NotNull
    public static Model<Object, PipelineCreateConfig, NodeClassificationPipeline> fromTrainingPipelineToModel(
        String username,
        String pipelineName,
        NodeClassificationPipeline pipeline
    ) {
        return Model.of(
            username,
            pipelineName,
            PIPELINE_MODEL_TYPE,
            GraphSchema.empty(),
            new Object(),
            PipelineCreateConfig.of(username),
            pipeline
        );
    }
}
