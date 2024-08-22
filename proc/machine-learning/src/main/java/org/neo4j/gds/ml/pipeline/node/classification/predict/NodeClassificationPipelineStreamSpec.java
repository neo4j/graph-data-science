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

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.models.BaseModelData;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineConstants.PREDICT_DESCRIPTION;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.predict.stream", description = PREDICT_DESCRIPTION, executionMode = STREAM)
public class NodeClassificationPipelineStreamSpec implements AlgorithmSpec<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineStreamConfig, Stream<NodeClassificationStreamResult>, NodeClassificationPredictPipelineAlgorithmFactory<NodeClassificationPredictPipelineStreamConfig>> {
    @Override
    public String name() {
        return "NodeClassificationPipelineStream";
    }

    @Override
    public NodeClassificationPredictPipelineAlgorithmFactory<NodeClassificationPredictPipelineStreamConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new NodeClassificationPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<NodeClassificationPredictPipelineStreamConfig> newConfigFunction() {
        return NodeClassificationPredictPipelineStreamConfig::of;
    }

    @Override
    public ComputationResultConsumer<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineStreamConfig, Stream<NodeClassificationStreamResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

    @Override
    public void preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        if (!userInput.containsKey("modelName")) return;

        var modelName = userInput.get("modelName");

        var model = executionContext.modelCatalog().get(
            executionContext.username(),
            (String) modelName,
            BaseModelData.class,
            NodeClassificationPipelineTrainConfig.class,
            Model.CustomInfo.class
        );

        if (!userInput.containsKey("targetNodeLabels")) userInput.put("targetNodeLabels", model.trainConfig().targetNodeLabels());
        if (!userInput.containsKey("relationshipTypes")) userInput.put("relationshipTypes", model.trainConfig().relationshipTypes());
    }
}
