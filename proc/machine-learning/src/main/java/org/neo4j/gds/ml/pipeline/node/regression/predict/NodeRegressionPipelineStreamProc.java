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
import org.neo4j.gds.StreamProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.ml.pipeline.PipelineCompanion.preparePipelineConfig;
import static org.neo4j.gds.ml.pipeline.node.regression.NodeRegressionProcCompanion.PREDICT_DESCRIPTION;
import static org.neo4j.gds.ml.pipeline.node.regression.predict.NodeRegressionPipelineStreamProc.NodeRegressionStreamResult;

@GdsCallable(name = "gds.alpha.pipeline.nodeRegression.predict.stream", description = PREDICT_DESCRIPTION, executionMode = STREAM)
public class NodeRegressionPipelineStreamProc
    extends StreamProc<
    NodeRegressionPredictPipelineExecutor,
    HugeDoubleArray,
    NodeRegressionStreamResult,
    NodeRegressionPredictPipelineBaseConfig>
{

    @Procedure(name = "gds.alpha.pipeline.nodeRegression.predict.stream", mode = Mode.READ)
    @Description(PREDICT_DESCRIPTION)
    public Stream<NodeRegressionStreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration") Map<String, Object> configuration
    ) {
        preparePipelineConfig(graphName, configuration);
        return stream(compute(graphName, configuration));
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineBaseConfig> computationResult) {
        return computationResult.result()
            .orElseGet(() -> HugeDoubleArray.newArray(0))
            .asNodeProperties();
    }

    @Override
    public AlgorithmFactory<?, NodeRegressionPredictPipelineExecutor, NodeRegressionPredictPipelineBaseConfig> algorithmFactory() {
        return new NodeRegressionPredictPipelineAlgorithmFactory<>(executionContext());
    }

    @Override
    public AlgorithmSpec<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineBaseConfig, Stream<NodeRegressionStreamResult>, AlgorithmFactory<?, NodeRegressionPredictPipelineExecutor, NodeRegressionPredictPipelineBaseConfig>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    @Override
    protected NodeRegressionPredictPipelineBaseConfig newConfig(String username, CypherMapWrapper config) {
        return newConfigFunction().apply(username, config);
    }

    @Override
    public NewConfigFunction<NodeRegressionPredictPipelineBaseConfig> newConfigFunction() {
        return new NodeRegressionPredictNewStreamConfigFn(modelCatalog());
    }

    @Override
    protected NodeRegressionStreamResult streamResult(
        long originalNodeId,
        long internalNodeId,
        NodePropertyValues nodePropertyValues
    ) {
        return new NodeRegressionStreamResult(originalNodeId, nodePropertyValues.doubleValue(internalNodeId));
    }


    @SuppressWarnings("unused")
    public static final class NodeRegressionStreamResult {

        public long nodeId;
        public double predictedValue;

        public NodeRegressionStreamResult(long nodeId, double predictedValue) {
            this.nodeId = nodeId;
            this.predictedValue = predictedValue;
        }
    }
}
