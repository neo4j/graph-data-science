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

import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.nodeClassification.NodeClassificationPredict;
import org.neo4j.gds.ml.pipeline.NodePropertyStepExecutor;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;

import java.util.List;

import static org.neo4j.gds.procedures.pipelines.NodeClassificationPredictPipelineConstants.MIN_BATCH_SIZE;

public class NodeClassificationPredictPipelineEstimator {
    private final ModelCatalog modelCatalog;

    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;

    public NodeClassificationPredictPipelineEstimator(
        ModelCatalog modelCatalog,
        AlgorithmsProcedureFacade algorithmsProcedureFacade
    ) {
        this.modelCatalog = modelCatalog;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
    }

    public MemoryEstimation estimate(
        Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> model,
        NodeClassificationPredictPipelineBaseConfig configuration
    ) {
        var pipeline = model.customInfo().pipeline();
        var classCount = model.customInfo().classes().size();
        var featureCount = model.data().featureDimension();

        var combinedNodeLabels = configuration.targetNodeLabels().isEmpty() ? model.trainConfig().targetNodeLabels() : configuration.targetNodeLabels();
        var combinedRelationshipTypes = configuration.relationshipTypes().isEmpty() ? model.trainConfig().relationshipTypes() : configuration.relationshipTypes();

        MemoryEstimation nodePropertyStepEstimation = NodePropertyStepExecutor.estimateNodePropertySteps(
            algorithmsProcedureFacade,
            modelCatalog,
            configuration.username(),
            pipeline.nodePropertySteps(),
            combinedNodeLabels,
            combinedRelationshipTypes
        );

        var predictionEstimation = MemoryEstimations.builder().add(
            "Pipeline Predict",
            NodeClassificationPredict.memoryEstimationWithDerivedBatchSize(
                model.data().trainerMethod(),
                configuration.includePredictedProbabilities(),
                MIN_BATCH_SIZE,
                featureCount,
                classCount,
                false
            )
        ).build();

        return MemoryEstimations.maxEstimation(List.of(nodePropertyStepEstimation, predictionEstimation));
    }
}
