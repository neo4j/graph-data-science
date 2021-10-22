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

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionTrainConfig;

import static org.neo4j.gds.ml.linkmodels.pipeline.LinkPredictionPipelineCreateProc.PIPELINE_MODEL_TYPE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PipelineUtils {

    private PipelineUtils() {}

    public static LinkPredictionPipelineBuilder getPipelineModelInfo(String pipelineName, String username) {
       var model = ModelCatalog.getUntyped(username, pipelineName);

        assert model != null;
        if (!model.algoType().equals(PIPELINE_MODEL_TYPE)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Steps can only be added to a model of type `%s`. But model `%s` is of type `%s`.",
                PIPELINE_MODEL_TYPE,
                pipelineName,
                model.algoType()
            ));
        }
        assert model.customInfo() instanceof LinkPredictionPipelineBuilder;

        return (LinkPredictionPipelineBuilder) model.customInfo();
    }

    public static Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> getLinkPredictionPipeline(
        String pipelineName,
        String username
    ) {
        return ModelCatalog.get(username, pipelineName, LinkLogisticRegressionData.class, LinkPredictionTrainConfig.class, LinkPredictionModelInfo.class);
    }
}
