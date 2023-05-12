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

import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.ml.pipeline.node.PredictMutateResult;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.ml.pipeline.node.regression.NodeRegressionProcCompanion.PREDICT_DESCRIPTION;

@GdsCallable(
    name = "gds.alpha.pipeline.nodeRegression.predict.mutate", description = PREDICT_DESCRIPTION,
    executionMode = MUTATE_NODE_PROPERTY
)
public class NodeRegressionPipelineMutateSpec
    implements AlgorithmSpec<
        NodeRegressionPredictPipelineExecutor,
        HugeDoubleArray,
        NodeRegressionPredictPipelineMutateConfig,
        Stream<PredictMutateResult>,
        NodeRegressionPredictPipelineAlgorithmFactory<NodeRegressionPredictPipelineMutateConfig>> {
    @Override
    public String name() {
        return "NodeRegressionPipelineMutate";
    }

    @Override
    public NodeRegressionPredictPipelineAlgorithmFactory<NodeRegressionPredictPipelineMutateConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new NodeRegressionPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<NodeRegressionPredictPipelineMutateConfig> newConfigFunction() {
        return NodeRegressionPredictPipelineMutateConfig::of;
    }

    @Override
    public void preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        NodeRegressionPipelineCompanion.enhanceUserInput(userInput, executionContext);
    }

    @Override
    public ComputationResultConsumer<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig, Stream<PredictMutateResult>> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(
            this::nodePropertyList,
            this::resultBuilder
        );
    }

    private List<NodeProperty> nodePropertyList(ComputationResult<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig> computationResult) {
        return List.of(NodeProperty.of(
            computationResult.config().mutateProperty(),
            nodeProperties(computationResult)
        ));
    }

    private NodePropertyValues nodeProperties(ComputationResult<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig> computationResult) {
        return computationResult.result()
            .orElseGet(() -> HugeDoubleArray.newArray(0))
            .asNodeProperties();
    }

    private PredictMutateResult.Builder resultBuilder(
        ComputationResult<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new PredictMutateResult.Builder();
    }
}
