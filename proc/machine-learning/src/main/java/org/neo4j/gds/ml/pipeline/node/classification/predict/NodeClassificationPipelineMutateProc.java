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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.MutatePropertyComputationResultConsumer;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.DoubleArrayNodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.ml.pipeline.node.PredictMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;
import static org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineCompanion.ESTIMATE_PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.pipeline.node.classification.NodeClassificationPipelineCompanion.PREDICT_DESCRIPTION;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.predict.mutate", description = PREDICT_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class NodeClassificationPipelineMutateProc
    extends MutatePropertyProc<
    NodeClassificationPredictPipelineExecutor,
    NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult,
    PredictMutateResult,
    NodeClassificationPredictPipelineMutateConfig>
{
    @Procedure(name = "gds.beta.pipeline.nodeClassification.predict.mutate", mode = Mode.READ)
    @Description(PREDICT_DESCRIPTION)
    public Stream<PredictMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        return mutate(compute(graphName, configuration));
    }

    @Procedure(name = "gds.beta.pipeline.nodeClassification.predict.mutate.estimate", mode = Mode.READ)
    @Description(ESTIMATE_PREDICT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        preparePipelineConfig(graphNameOrConfiguration, algoConfiguration);
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public AlgorithmSpec<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig, Stream<PredictMutateResult>, AlgorithmFactory<?, NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineMutateConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @Override
    public MutatePropertyComputationResultConsumer<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig, PredictMutateResult> computationResultConsumer() {
        return new MutatePropertyComputationResultConsumer<>(this::nodePropertyList, this::resultBuilder) {
            @Override
            protected Graph graphFromComputationResult(ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig> computationResult) {
                return computationResult.graphStore().getGraph(computationResult.algorithm().nodePropertyStepFilter().nodeLabels());
            }
        };
    }

    @Override
    protected List<NodeProperty> nodePropertyList(ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig> computationResult) {
        var config = computationResult.config();
        var mutateProperty = config.mutateProperty();
        var result = computationResult.result();
        var classProperties = result.predictedClasses().asNodeProperties();
        var nodeProperties = new ArrayList<NodeProperty>();
        nodeProperties.add(NodeProperty.of(mutateProperty, classProperties));

        result.predictedProbabilities().ifPresent(probabilityProperties -> {
            var properties = new DoubleArrayNodePropertyValues() {
                @Override
                public long size() {
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


    @Override
    protected AbstractResultBuilder<PredictMutateResult> resultBuilder(
        ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineExecutor.NodeClassificationPipelineResult, NodeClassificationPredictPipelineMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new PredictMutateResult.Builder();
    }

    @Override
    protected NodeClassificationPredictPipelineMutateConfig newConfig(String username, CypherMapWrapper config) {
        return NodeClassificationPredictPipelineMutateConfig.of(username, config);
    }

    @Override
    public GraphStoreAlgorithmFactory<NodeClassificationPredictPipelineExecutor, NodeClassificationPredictPipelineMutateConfig> algorithmFactory() {
        return new NodeClassificationPredictPipelineAlgorithmFactory<>(executionContext(), modelCatalog());
    }

}
