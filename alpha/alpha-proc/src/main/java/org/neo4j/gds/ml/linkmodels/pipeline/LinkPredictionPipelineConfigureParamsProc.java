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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfigImpl;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LinkPredictionPipelineConfigureParamsProc extends BaseProc {

    @Context
    public ModelCatalog modelCatalog;

    @Procedure(name = "gds.alpha.ml.pipeline.linkPrediction.configureParams", mode = READ)
    @Description("Configures the parameters of the link prediction train pipeline.")
    public Stream<PipelineInfoResult> configureParams(@Name("pipelineName") String pipelineName, @Name("parameterSpace") List<Map<String, Object>> parameterSpace) {
        var pipeline = LinkPredictionPipelineCompanion.getLPPipeline(modelCatalog, pipelineName, username());

        List<LinkLogisticRegressionTrainConfig> trainConfigs = parameterSpace
            .stream()
            .map(CypherMapWrapper::create)
            .map(rawConfig -> {
                var config = new LinkLogisticRegressionTrainConfigImpl(rawConfig);
                validateConfig(rawConfig, config.configKeys());
                return config;
            })
            .collect(Collectors.toList());

        pipeline.setTrainingParameterSpace(trainConfigs);

        return Stream.of(new PipelineInfoResult(pipelineName, pipeline));
    }
}
