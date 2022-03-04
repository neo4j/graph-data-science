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
import org.neo4j.gds.models.logisticregression.LogisticRegressionData;
import org.neo4j.gds.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeClassificationPipelineTrainConfig;

import java.util.List;
import java.util.Map;

public final class NodeClassificationPipelineCompanion {
    public static final String PREDICT_DESCRIPTION = "Predicts classes for all nodes based on a previously trained pipeline model";
    public static final String ESTIMATE_PREDICT_DESCRIPTION = "Estimates memory for predicting classes for all nodes based on a previously trained pipeline model";
    static final Map<String, Object> DEFAULT_SPLIT_CONFIG =  Map.of("testFraction", 0.3, "validationFolds", 3);
    static final List<Map<String, Object>> DEFAULT_PARAM_CONFIG = List.of(
        LogisticRegressionTrainConfig.defaultConfig().toMap()
    );

    private NodeClassificationPipelineCompanion() {}

    public static Model<LogisticRegressionData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> getTrainedNCPipelineModel(
        ModelCatalog modelCatalog,
        String modelName,
        String username
    ) {
        return modelCatalog.get(
            username,
            modelName,
            LogisticRegressionData.class,
            NodeClassificationPipelineTrainConfig.class,
            NodeClassificationPipelineModelInfo.class
        );
    }
}
