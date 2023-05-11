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

import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.ml.models.BaseModelData;
import org.neo4j.gds.ml.pipeline.node.PredictMutateResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineConstants.PREDICT_DESCRIPTION;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.predict.mutate", description = PREDICT_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class NodeClassificationPipelineMutateSpec implements AlgorithmSpec<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig, Stream<PredictMutateResult>, NodeClassificationPredictPipelineAlgorithmFactory<NodeClassificationPredictPipelineMutateConfig>> {
    @Override
    public String name() {
        return "NodeClassificationPipelineMutate";
    }

    @Override
    public NodeClassificationPredictPipelineAlgorithmFactory<NodeClassificationPredictPipelineMutateConfig> algorithmFactory(ExecutionContext executionContext) {
        return new NodeClassificationPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<NodeClassificationPredictPipelineMutateConfig> newConfigFunction() {
        return NodeClassificationPredictPipelineMutateConfig::of;
    }

    @Override
    public ComputationResultConsumer<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig, Stream<PredictMutateResult>> computationResultConsumer() {
        var resultBuilderFunction = new NodeClassificationPredictPipelineMutateResultBuilderFunction();

        return new NodeClassificationPredictPipelineMutateComputationResultConsumer(
            resultBuilderFunction,
            NodeClassificationPipelineMutateSpec::nodePropertyList
        );
    }

    @Override
    public void preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        if (! userInput.containsKey("modelName")) return;

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

    private static List<NodeProperty> nodePropertyList(ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig> computationResult) {
        if (computationResult.result().isEmpty()) return Collections.emptyList();

        var config = computationResult.config();
        var mutateProperty = config.mutateProperty();
        var result = computationResult.result().get();
        var classProperties = result.predictedClasses().asNodeProperties();
        var nodeProperties = new ArrayList<NodeProperty>();

        nodeProperties.add(NodeProperty.of(mutateProperty, classProperties));

        result.predictedProbabilities().ifPresent(probabilityProperties -> {
            var properties = new DoubleArrayNodePropertyValues() {
                @Override
                public long nodeCount() {
                    return computationResult.graph().nodeCount();
                }

                @Override
                public double[] doubleArrayValue(long nodeId) {
                    return probabilityProperties.get(nodeId);
                }
            };

            nodeProperties.add(NodeProperty.of(
                config.predictedProbabilityProperty().orElseThrow(),
                properties
            ));
        });

        return nodeProperties;
    }
}
