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

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;

import java.util.List;

public class NodeClassificationPredictPipelineFilterUtil {

    public static PredictGraphFilter generatePredictPipelineFilter(ModelCatalog modelCatalog, String modelName, String username, NodeClassificationPredictPipelineBaseConfig basePredictConfig) {
        var trainedModel = modelCatalog.get(
            username,
            modelName,
            Classifier.ClassifierData.class,
            NodeClassificationPipelineTrainConfig.class,
            NodeClassificationPipelineModelInfo.class);

        var combinedTargetNodeLabels = basePredictConfig.targetNodeLabels().isEmpty() ? trainedModel.trainConfig().targetNodeLabels() : basePredictConfig.targetNodeLabels();
        var combinedRelationshipTypes = basePredictConfig.relationshipTypes().isEmpty() ? trainedModel.trainConfig().relationshipTypes() : basePredictConfig.relationshipTypes();
        return ImmutablePredictGraphFilter.builder()
            .nodeLabels(combinedTargetNodeLabels)
            .relationshipTypes(combinedRelationshipTypes)
            .build();
    }

    @ValueClass
    public interface PredictGraphFilter {
        List<String> nodeLabels();
        List<String> relationshipTypes();
    }

}
