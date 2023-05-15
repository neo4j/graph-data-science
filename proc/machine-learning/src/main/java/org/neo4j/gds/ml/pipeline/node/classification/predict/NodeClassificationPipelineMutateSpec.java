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

import org.neo4j.gds.GraphStoreUpdater;
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.ml.pipeline.node.PredictMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;

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
        return new MutateComputationResultConsumer<>((computationResult, executionContext) -> new PredictMutateResult.Builder()) {
            @Override
            protected void updateGraphStore(
                AbstractResultBuilder<?> resultBuilder,
                ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig> computationResult,
                ExecutionContext executionContext
            ) {
                GraphStoreUpdater.UpdateGraphStore(
                    resultBuilder,
                    computationResult,
                    executionContext,
                    PredictedProbabilities.asProperties(
                        computationResult.result(),
                        computationResult.config().mutateProperty(),
                        computationResult.config().predictedProbabilityProperty()
                    )
                );
            }
        };
    }

    @Override
    public void preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        NodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(userInput, executionContext);
    }
}
