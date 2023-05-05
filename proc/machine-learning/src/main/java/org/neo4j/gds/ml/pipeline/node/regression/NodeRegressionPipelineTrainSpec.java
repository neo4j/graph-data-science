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
package org.neo4j.gds.ml.pipeline.node.regression;

import org.neo4j.gds.VerifyThatModelCanBeStored;
import org.neo4j.gds.compat.ProxyUtil;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainPipelineAlgorithmFactory;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;

@GdsCallable(name = "gds.alpha.pipeline.nodeRegression.train", description = "Trains a node regression model based on a pipeline", executionMode = TRAIN)
public class NodeRegressionPipelineTrainSpec implements AlgorithmSpec<
    NodeRegressionTrainAlgorithm,
    NodeRegressionTrainResult.NodeRegressionTrainPipelineResult,
    NodeRegressionPipelineTrainConfig,
    Stream<TrainResult>,
    NodeRegressionTrainPipelineAlgorithmFactory> {
    @Override
    public String name() {
        return "NodeRegressionPipelineTrain";
    }

    @Override
    public NodeRegressionTrainPipelineAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        var gdsVersion = ProxyUtil.GDS_VERSION_INFO.gdsVersion();
        return new NodeRegressionTrainPipelineAlgorithmFactory(executionContext, gdsVersion);
    }

    @Override
    public NewConfigFunction<NodeRegressionPipelineTrainConfig> newConfigFunction() {
        return NodeRegressionPipelineTrainConfig::of;
    }

    @Override
    public ComputationResultConsumer<NodeRegressionTrainAlgorithm, NodeRegressionTrainResult.NodeRegressionTrainPipelineResult, NodeRegressionPipelineTrainConfig, Stream<TrainResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            return computationResult.result().map(result -> {
                var model = result.model();
                var modelCatalog = executionContext.modelCatalog();
                assert modelCatalog != null : "ModelCatalog should have been set in the ExecutionContext by this point!!!";
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
                return Stream.of(new TrainResult(model, result.trainingStatistics(), computationResult.computeMillis()
                ));
            }).orElseGet(Stream::empty);
        };
    }

    @Override
    public ValidationConfiguration<NodeRegressionPipelineTrainConfig> validationConfig(ExecutionContext executionContext) {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<NodeRegressionPipelineTrainConfig>> beforeLoadValidations() {
                var modelCatalog = executionContext.modelCatalog();
                assert modelCatalog != null : "ModelCatalog should have been set in the ExecutionContext by this point!!!";
                return List.of(
                    new VerifyThatModelCanBeStored<>(
                        modelCatalog,
                        executionContext.username(),
                        LinkPredictionTrainingPipeline.MODEL_TYPE
                    )
                );
            }
        };
    }

}
