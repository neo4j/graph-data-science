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

import org.neo4j.gds.WriteNodePropertiesComputationResultConsumer;
import org.neo4j.gds.WriteNodePropertyListFunction;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.ml.pipeline.node.classification.predict.NodeClassificationPipelineConstants.PREDICT_DESCRIPTION;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.predict.write", description = PREDICT_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class NodeClassificationPipelineWriteSpec implements AlgorithmSpec<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineWriteConfig, Stream<WriteResult>, NodeClassificationPredictPipelineAlgorithmFactory<NodeClassificationPredictPipelineWriteConfig>> {
    @Override
    public String name() {
        return "NodeClassificationPipelineWrite";
    }

    @Override
    public NodeClassificationPredictPipelineAlgorithmFactory<NodeClassificationPredictPipelineWriteConfig> algorithmFactory(ExecutionContext executionContext) {
        return new NodeClassificationPredictPipelineAlgorithmFactory<>(executionContext);
    }

    @Override
    public NewConfigFunction<NodeClassificationPredictPipelineWriteConfig> newConfigFunction() {
        return NodeClassificationPredictPipelineWriteConfig::of;
    }

    @Override
    public ComputationResultConsumer<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineWriteConfig, Stream<WriteResult>> computationResultConsumer() {
        var writeNodePropertyListFunction = new WriteNodePropertyListFunction<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineWriteConfig>() {
            @Override
            public List<NodeProperty> apply(ComputationResult<NodeClassificationPredictPipelineExecutor, NodeClassificationPipelineResult, NodeClassificationPredictPipelineWriteConfig> computationResult) {
                return PredictedProbabilities.asProperties(
                    computationResult.result(),
                    computationResult.config().writeProperty(),
                    computationResult.config()
                );
            }
        };

        return new WriteNodePropertiesComputationResultConsumer<>(
            (computationResult, executionContext) -> new WriteResult.Builder(),
            writeNodePropertyListFunction,
            name()
        );
    }

    @Override
    public void preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        ConfigPreProcessor.Enhance(userInput, executionContext);
    }
}
