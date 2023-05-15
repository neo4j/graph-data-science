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
package org.neo4j.gds.ml.pipeline.node.classification.predict;

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.ml.models.BaseModelData;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;

import java.util.Map;

/**
 * Enhance user input by adding targetNodeLabels and relationshipTypes from training parameters if appropriate
 */
final class NodeClassificationPredictConfigPreProcessor {
    private NodeClassificationPredictConfigPreProcessor() {}

    static void enhanceInputWithPipelineParameters(Map<String, Object> userInput, ExecutionContext executionContext) {
        if (!userInput.containsKey("modelName")) return;

        //noinspection ConstantConditions
        var model = executionContext.modelCatalog().get(
            executionContext.username(),
            (String) userInput.get("modelName"),
            BaseModelData.class,
            NodeClassificationPipelineTrainConfig.class,
            Model.CustomInfo.class
        );

        userInput.putIfAbsent("targetNodeLabels", model.trainConfig().targetNodeLabels());
        userInput.putIfAbsent("relationshipTypes", model.trainConfig().relationshipTypes());
    }
}
