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
package org.neo4j.gds.ml.pipeline.node.regression.predict;

import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;

import java.util.List;

public class NodeRegressionPredictPipelineFilterUtil {

    public static List<List<String>> generatePredictPipelineFilter(ModelCatalog modelCatalog, String modelName, String username, NodeRegressionPredictPipelineBaseConfig basePredictConfig) {
        var trainedModel = modelCatalog.get(
            username,
            modelName,
            Regressor.RegressorData.class,
            NodeRegressionPipelineTrainConfig.class,
            NodeRegressionPipelineModelInfo.class);

        var combinedTargetNodeLabels = basePredictConfig.targetNodeLabels().isEmpty() ? trainedModel.trainConfig().targetNodeLabels() : basePredictConfig.targetNodeLabels();
        var combinedRelationshipTypes = basePredictConfig.relationshipTypes().isEmpty() ? trainedModel.trainConfig().relationshipTypes() : basePredictConfig.relationshipTypes();
        return List.of(combinedTargetNodeLabels, combinedRelationshipTypes);
    }
}
