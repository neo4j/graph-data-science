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

import org.neo4j.gds.VerifyThatModelCanBeStored;
import org.neo4j.gds.compat.ProxyUtil;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationModelResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrainPipelineAlgorithmFactory;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;

@GdsCallable(name = "gds.beta.pipeline.nodeClassification.train", description = "Trains a node classification model based on a pipeline", executionMode = TRAIN)
public class NodeClassificationPipelineTrainSpec implements AlgorithmSpec<NodeClassificationTrainAlgorithm, NodeClassificationModelResult, NodeClassificationPipelineTrainConfig, Stream<NodeClassificationPipelineTrainResult>, NodeClassificationTrainPipelineAlgorithmFactory> {

    @Override
    public String name() {
        return "NodeClassificationPipelineTrain";
    }

    @Override
    public NodeClassificationTrainPipelineAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        return new NodeClassificationTrainPipelineAlgorithmFactory(executionContext, ProxyUtil.GDS_VERSION_INFO.gdsVersion());
    }

    @Override
    public NewConfigFunction<NodeClassificationPipelineTrainConfig> newConfigFunction() {
        return NodeClassificationPipelineTrainConfig::of;
    }

    @Override
    public ComputationResultConsumer<NodeClassificationTrainAlgorithm, NodeClassificationModelResult, NodeClassificationPipelineTrainConfig, Stream<NodeClassificationPipelineTrainResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            if (computationResult.result().isPresent()) {
                var model = (Model<?, ?, ?>) computationResult.result().get().model();
                var modelCatalog = executionContext.modelCatalog();
                modelCatalog.set(model);

                if (computationResult.config().storeModelToDisk()) {
                    try {
                        // FIXME: This works but is not what we want to do!
                        var databaseService = executionContext.dependencyResolver()
                            .resolveDependency(GraphDatabaseService.class);

                        modelCatalog.checkLicenseBeforeStoreModel(databaseService, "Store a model");
                        var modelDir = modelCatalog.getModelDirectory(databaseService);
                        modelCatalog.store(model.creator(), model.name(), modelDir);
                    } catch (Exception e) {
                        executionContext.log().error("Failed to store model to disk after training.", e.getMessage());
                        throw e;
                    }
                }
                return Stream.of(constructProcResult(computationResult));
            }

            return Stream.empty();
        };
    }

    @Override
    public ValidationConfiguration<NodeClassificationPipelineTrainConfig> validationConfig(ExecutionContext executionContext) {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<NodeClassificationPipelineTrainConfig>> beforeLoadValidations() {
                return List.of(
                    new VerifyThatModelCanBeStored<>(executionContext.modelCatalog(), executionContext.username(), NodeClassificationTrainingPipeline.MODEL_TYPE)
                );
            }
        };
    }

    private NodeClassificationPipelineTrainResult constructProcResult(
        ComputationResult<
            NodeClassificationTrainAlgorithm,
            NodeClassificationModelResult,
            NodeClassificationPipelineTrainConfig> computationResult
    ) {
        var transformedResult = computationResult.result();
        return new NodeClassificationPipelineTrainResult(transformedResult, computationResult.computeMillis());
    }
}
