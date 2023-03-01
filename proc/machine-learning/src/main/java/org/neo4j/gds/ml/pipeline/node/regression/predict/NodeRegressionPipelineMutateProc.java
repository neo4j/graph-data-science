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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.ml.pipeline.node.PredictMutateResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;
import static org.neo4j.gds.ml.pipeline.node.regression.NodeRegressionProcCompanion.PREDICT_DESCRIPTION;

@GdsCallable(name = "gds.alpha.pipeline.nodeRegression.predict.mutate", description = PREDICT_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class NodeRegressionPipelineMutateProc
    extends MutatePropertyProc<
    NodeRegressionPredictPipelineExecutor,
    HugeDoubleArray,
    PredictMutateResult,
    NodeRegressionPredictPipelineMutateConfig> {

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.predict.mutate", mode = Mode.READ)
    @Description(PREDICT_DESCRIPTION)
    public Stream<PredictMutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        return mutate(compute(graphName, configuration));
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig> computationResult) {
        var size = computationResult.graph().nodeCount();
        var predictedPropertyValues = computationResult.result();

        return new DoubleNodePropertyValues() {
            @Override
            public long valuesStored() {
                return size;
            }

            @Override
            public double doubleValue(long nodeId) {
                return predictedPropertyValues.get(nodeId);
            }
        };
    }

    @Override
    public AlgorithmSpec<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig, Stream<PredictMutateResult>, AlgorithmFactory<?, NodeRegressionPredictPipelineExecutor, NodeRegressionPredictPipelineMutateConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @Override
    protected AbstractResultBuilder<PredictMutateResult> resultBuilder(
        ComputationResult<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new PredictMutateResult.Builder();
    }

    @Override
    protected NodeRegressionPredictPipelineMutateConfig newConfig(String username, CypherMapWrapper config) {
        return newConfigFunction().apply(username, config);
    }

    @Override
    public NewConfigFunction<NodeRegressionPredictPipelineMutateConfig> newConfigFunction() {
        return new NodeRegressionPredictNewMutateConfigFn(modelCatalog());
    }

    @Override
    public GraphStoreAlgorithmFactory<NodeRegressionPredictPipelineExecutor, NodeRegressionPredictPipelineMutateConfig> algorithmFactory() {
        return new NodeRegressionPredictPipelineAlgorithmFactory<>(executionContext(), modelCatalog());
    }

}
