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

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.model.OpenModelCatalog;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionTrainCoreConfig;

import java.util.List;
import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeClassificationPipelineCompanion {
    public static final String PIPELINE_MODEL_TYPE = "Node classification training pipeline";
    static final Map<String, Object> DEFAULT_SPLIT_CONFIG =  Map.of("holdoutFraction", 0.3, "validationFolds", 3);
    static final List<Map<String, Object>> DEFAULT_PARAM_CONFIG = List.of(
        NodeLogisticRegressionTrainCoreConfig.defaultConfig().toMap()
    );

    private NodeClassificationPipelineCompanion() {}

    public static NodeClassificationPipeline getNCPipeline(ModelCatalog modelCatalog, String pipelineName, String username) {
        var model = modelCatalog.getUntypedOrThrow(username, pipelineName);

        assert model != null;
        if (!model.algoType().equals(PIPELINE_MODEL_TYPE)) {
            throw new IllegalArgumentException(formatWithLocale(
                "Steps can only be added to a model of type `%s`. But model `%s` is of type `%s`.",
                PIPELINE_MODEL_TYPE,
                pipelineName,
                model.algoType()
            ));
        }
        assert model.customInfo() instanceof NodeClassificationPipeline;

        return (NodeClassificationPipeline) model.customInfo();
    }

    public static Model<NodeLogisticRegressionData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> getTrainedNCPipelineModel(
        ModelCatalog modelCatalog,
        String pipelineName,
        String username
    ) {
        return modelCatalog.get(
            username,
            pipelineName,
            NodeLogisticRegressionData.class,
            NodeClassificationPipelineTrainConfig.class,
            NodeClassificationPipelineModelInfo.class
        );
    }
}
