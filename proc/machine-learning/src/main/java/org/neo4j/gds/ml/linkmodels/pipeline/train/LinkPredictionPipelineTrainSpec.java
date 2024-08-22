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
package org.neo4j.gds.ml.linkmodels.pipeline.train;

import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.VerifyThatModelCanBeStored;
import org.neo4j.gds.compat.GdsVersionInfoProvider;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.train.LinkPredictionTrainPipelineExecutor;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;

import java.util.List;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.TRAIN;

@GdsCallable(name = "gds.beta.pipeline.linkPrediction.train", description = "Trains a link prediction model based on a pipeline", executionMode = TRAIN)
public class LinkPredictionPipelineTrainSpec implements AlgorithmSpec<
    LinkPredictionTrainPipelineExecutor,
    LinkPredictionTrainPipelineExecutor.LinkPredictionTrainPipelineResult,
    LinkPredictionTrainConfig,
    Stream<TrainResult>,
    LinkPredictionTrainPipelineAlgorithmFactory
    > {
    @Override
    public String name() {
        return "LinkPredictionPipelineTrain";
    }

    @Override
    public LinkPredictionTrainPipelineAlgorithmFactory algorithmFactory(ExecutionContext executionContext) {
        var gdsVersion = GdsVersionInfoProvider.GDS_VERSION_INFO.gdsVersion();
        return new LinkPredictionTrainPipelineAlgorithmFactory(executionContext, gdsVersion);
    }

    @Override
    public NewConfigFunction<LinkPredictionTrainConfig> newConfigFunction() {
        return LinkPredictionTrainConfig::of;
    }

    @Override
    public ComputationResultConsumer<LinkPredictionTrainPipelineExecutor, LinkPredictionTrainPipelineExecutor.LinkPredictionTrainPipelineResult, LinkPredictionTrainConfig, Stream<TrainResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }

    @Override
    public ValidationConfiguration<LinkPredictionTrainConfig> validationConfig(ExecutionContext executionContext) {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<LinkPredictionTrainConfig>> beforeLoadValidations() {
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
