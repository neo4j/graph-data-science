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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.procedures.pipelines.NodeRegressionPredictConfigPreProcessor;
import org.neo4j.gds.procedures.pipelines.NodeRegressionPredictPipelineBaseConfig;
import org.neo4j.gds.procedures.pipelines.NodeRegressionPredictPipelineExecutor;
import org.neo4j.gds.procedures.pipelines.NodeRegressionStreamResult;

import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;
import static org.neo4j.gds.executor.ExecutionMode.STREAM;
import static org.neo4j.gds.ml.pipeline.node.regression.NodeRegressionProcCompanion.PREDICT_DESCRIPTION;

@GdsCallable(
    name = "gds.alpha.pipeline.nodeRegression.predict.stream", description = PREDICT_DESCRIPTION,
    executionMode = STREAM
)
public class NodeRegressionPipelineStreamSpec
    implements AlgorithmSpec<
        NodeRegressionPredictPipelineExecutor,
        HugeDoubleArray,
        NodeRegressionPredictPipelineBaseConfig,
        Stream<NodeRegressionStreamResult>,
        NodeRegressionPredictPipelineAlgorithmFactory<NodeRegressionPredictPipelineBaseConfig>> {
    @Override
    public String name() {
        return "NodeRegressionPipelineStream";
    }

    @Override
    public NodeRegressionPredictPipelineAlgorithmFactory<NodeRegressionPredictPipelineBaseConfig> algorithmFactory(
        ExecutionContext executionContext
    ) {
        return new NodeRegressionPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<NodeRegressionPredictPipelineBaseConfig> newConfigFunction() {
        return NodeRegressionPredictPipelineBaseConfig::of;
    }

    @Override
    public void preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        NodeRegressionPredictConfigPreProcessor.enhanceInputWithPipelineParameters(userInput, executionContext);
    }

    @Override
    public ComputationResultConsumer<NodeRegressionPredictPipelineExecutor, HugeDoubleArray, NodeRegressionPredictPipelineBaseConfig, Stream<NodeRegressionStreamResult>> computationResultConsumer() {
        return (computationResult, executionContext) ->
            runWithExceptionLogging(
                "Result streaming failed",
                executionContext.log(),
                () -> computationResult.result()
                    .map(result -> {
                        Graph graph = computationResult.graph();
                        NodePropertyValues nodePropertyValues = NodePropertyValuesAdapter.adapt(result);
                        return LongStream
                            .range(IdMap.START_NODE_ID, graph.nodeCount())
                            .filter(nodePropertyValues::hasValue)
                            .mapToObj(nodeId -> new NodeRegressionStreamResult(
                                graph.toOriginalNodeId(nodeId),
                                nodePropertyValues.doubleValue(nodeId)
                            ));
                    }).orElseGet(Stream::empty)
            );
    }
}
