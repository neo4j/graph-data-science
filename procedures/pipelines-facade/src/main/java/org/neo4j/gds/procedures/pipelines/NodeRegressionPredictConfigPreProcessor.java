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

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.models.BaseModelData;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPipelineBaseTrainConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NodeRegressionPredictConfigPreProcessor {
    private final ModelCatalog modelCatalog;
    private final User user;

    NodeRegressionPredictConfigPreProcessor(ModelCatalog modelCatalog, User user) {
        this.modelCatalog = modelCatalog;
        this.user = user;
    }

    void enhanceInputWithPipelineParameters(Map<String, Object> userInput) {
        Optional.ofNullable(userInput.get("modelName"))
            .map(modelName -> {
                assert modelCatalog != null : "ModelCatalog should have been set in the ExecutionContext by this point!!!";

                var trainedModel = modelCatalog.get(
                    user.getUsername(),
                    (String) modelName,
                    BaseModelData.class,
                    NodePropertyPipelineBaseTrainConfig.class,
                    Model.CustomInfo.class
                );

                var combinedTargetNodeLabels = Optional.ofNullable(userInput.get("targetNodeLabels"))
                    .map(targetNodeLabels -> (List<String>) targetNodeLabels)
                    .orElseGet(() -> trainedModel.trainConfig().targetNodeLabels());

                var combinedRelationshipTypes = Optional.ofNullable(userInput.get("relationshipTypes"))
                    .map(relationshipTypes -> (List<String>) relationshipTypes)
                    .orElseGet(() -> trainedModel.trainConfig().relationshipTypes());

                userInput.put("targetNodeLabels", combinedTargetNodeLabels);
                userInput.put("relationshipTypes", combinedRelationshipTypes);
                return userInput;
            });
    }
}
