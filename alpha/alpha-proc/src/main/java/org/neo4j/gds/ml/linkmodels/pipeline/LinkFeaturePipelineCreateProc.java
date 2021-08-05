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
package org.neo4j.gds.ml.linkmodels.pipeline;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.ModelConfig;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LinkFeaturePipelineCreateProc extends BaseProc {
    public static final String PIPELINE_MODEL_TYPE = "Link prediction training pipeline";


    @Procedure(name = "gds.alpha.ml.pipeline.linkPrediction.create", mode = READ)
    @Description("Creates a link prediction pipeline in the model catalog.")
    public Stream<PipelineInfoResult> create(@Name("pipelineName") String pipelineName) {
        var model = Model.of(
            username(),
            pipelineName,
            PIPELINE_MODEL_TYPE,
            GraphSchema.empty(),
            new Object(),
            PipelineDummyTrainConfig.of(username()),
            new PipelineModelInfo()
        );

        ModelCatalog.set(model);

        return Stream.of(new PipelineInfoResult(pipelineName, (PipelineModelInfo) model.customInfo()));
    }

    @ValueClass
    @Configuration
    interface PipelineDummyTrainConfig extends BaseConfig, ModelConfig {
        static PipelineDummyTrainConfig of(String username) {
            return ImmutablePipelineDummyTrainConfig.of(username, "");
        }
    }
}
