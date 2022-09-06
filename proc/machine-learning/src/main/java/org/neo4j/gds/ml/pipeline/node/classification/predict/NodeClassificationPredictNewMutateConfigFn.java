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

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.NewConfigFunction;

import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPredictPipelineFilterUtil.generatePredictPipelineFilter;

public class NodeClassificationPredictNewMutateConfigFn implements NewConfigFunction<NodeClassificationPredictPipelineMutateConfig> {

    private final ModelCatalog modelCatalog;

    NodeClassificationPredictNewMutateConfigFn(ModelCatalog modelCatalog) {
        this.modelCatalog = modelCatalog;
    }

    @Override
    public NodeClassificationPredictPipelineMutateConfig apply(String username, CypherMapWrapper config) {
        var basePredictConfig = NodeClassificationPredictPipelineMutateConfig.of(username, config);
        var modelName = config.getString("modelName");
        if (modelName.isEmpty()) {
            return basePredictConfig;
        } else {
            var combinedFilter = generatePredictPipelineFilter(modelCatalog, modelName.get(), username, basePredictConfig);

            return NodeClassificationPredictPipelineMutateConfigImpl.builder()
                .graphName(basePredictConfig.graphName())
                .modelName(modelName.get())
                .concurrency(basePredictConfig.concurrency())
                .jobId(basePredictConfig.jobId())
                .modelUser(basePredictConfig.modelUser())
                .targetNodeLabels(combinedFilter.get(0))
                .relationshipTypes(combinedFilter.get(1))
                .predictedProbabilityProperty(basePredictConfig.predictedProbabilityProperty())
                .mutateProperty(basePredictConfig.mutateProperty())
                .build();
        }
    }

}

