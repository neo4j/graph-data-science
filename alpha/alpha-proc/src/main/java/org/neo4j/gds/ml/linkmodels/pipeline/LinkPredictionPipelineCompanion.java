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
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionData;
import org.neo4j.gds.ml.linkmodels.pipeline.logisticRegression.LinkLogisticRegressionTrainConfig;
import org.neo4j.gds.ml.linkmodels.pipeline.train.LinkPredictionTrainConfig;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class LinkPredictionPipelineCompanion {

    public static final String PREDICT_DESCRIPTION = "Predicts relationships for all node pairs based on a previously trained link prediction model.";
    public static final String PIPELINE_MODEL_TYPE = "Link prediction training pipeline";
    static final List<Map<String, Object>> DEFAULT_PARAM_CONFIG = List.of(
        LinkLogisticRegressionTrainConfig.defaultConfig().toMap()
    );
    private static final ModelCatalog modelCatalog = OpenModelCatalog.INSTANCE;

    private LinkPredictionPipelineCompanion() {}

    public static LinkPredictionPipeline getLPPipeline(String pipelineName, String username) {
       var model = modelCatalog.getUntyped(username, pipelineName);

        assert model != null;
        if (!model.algoType().equals(PIPELINE_MODEL_TYPE)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Steps can only be added to a model of type `%s`. But model `%s` is of type `%s`.",
                PIPELINE_MODEL_TYPE,
                pipelineName,
                model.algoType()
            ));
        }
        assert model.customInfo() instanceof LinkPredictionPipeline;

        return (LinkPredictionPipeline) model.customInfo();
    }

    public static Model<LinkLogisticRegressionData, LinkPredictionTrainConfig, LinkPredictionModelInfo> getTrainedLPPipelineModel(
        String pipelineName,
        String username
    ) {
        return modelCatalog.get(username, pipelineName, LinkLogisticRegressionData.class, LinkPredictionTrainConfig.class, LinkPredictionModelInfo.class);
    }
}
