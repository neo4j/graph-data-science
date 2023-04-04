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
package org.neo4j.gds;

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.Model.CustomInfo;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.validation.BeforeLoadValidation;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.ml.training.TrainBaseConfig;
import org.neo4j.gds.model.ModelConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.model.ModelConfig.MODEL_NAME_KEY;
import static org.neo4j.gds.model.ModelConfig.MODEL_TYPE_KEY;

public abstract class TrainProc<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    TRAIN_CONFIG extends TrainBaseConfig,
    PROC_RESULT
    > extends AlgoBaseProc<ALGO, ALGO_RESULT, TRAIN_CONFIG, PROC_RESULT> {

    protected abstract String modelType();

    protected abstract PROC_RESULT constructProcResult(ComputationResult<ALGO, ALGO_RESULT, TRAIN_CONFIG> computationResult);

    protected abstract Model<?, ?, ?> extractModel(ALGO_RESULT algo_result);

    @Override
    public ComputationResultConsumer<ALGO, ALGO_RESULT, TRAIN_CONFIG, Stream<PROC_RESULT>> computationResultConsumer() {
        return (computationResult, executionContext) -> {
            var model = extractModel(computationResult.result());
            var modelCatelog = modelCatalog();
            modelCatalog().set(model);

            if (computationResult.config().storeModelToDisk()) {
                try {
                    modelCatelog.checkLicenseBeforeStoreModel(databaseService, "Store a model");
                    var modelDir = modelCatelog.getModelDirectory(databaseService);
                    modelCatelog.store(model.creator(), model.name(), modelDir);
                } catch (Exception e) {
                    log.error("Failed to store model to disk after training.", e.getMessage());
                    throw e;
                }
            }
            return Stream.of(constructProcResult(computationResult));
        };
    }

    protected Stream<PROC_RESULT> trainAndSetModelWithResult(ComputationResult<ALGO, ALGO_RESULT, TRAIN_CONFIG> computationResult) {
        return computationResultConsumer().consume(computationResult, executionContext());
    }

    @Override
    public ValidationConfiguration<TRAIN_CONFIG> validationConfig(ExecutionContext executionContext) {
        return new ValidationConfiguration<>() {
            @Override
            public List<BeforeLoadValidation<TRAIN_CONFIG>> beforeLoadValidations() {
                return List.of(
                   new TrainingConfigValidation<>(modelCatalog(), username(), modelType())
                );
            }
        };
    }

    @Override
    public AlgorithmSpec<ALGO, ALGO_RESULT, TRAIN_CONFIG, Stream<PROC_RESULT>, AlgorithmFactory<?, ALGO, TRAIN_CONFIG>> withModelCatalog(
        ModelCatalog modelCatalog
    ) {
        this.setModelCatalog(modelCatalog);
        return this;
    }

    public static class TrainingConfigValidation<TRAIN_CONFIG extends ModelConfig & AlgoBaseConfig> implements BeforeLoadValidation<TRAIN_CONFIG> {
        private final ModelCatalog modelCatalog;
        private final String username;
        private final String modelType;

        public TrainingConfigValidation(ModelCatalog modelCatalog, String username, String modelType) {
            this.modelCatalog = modelCatalog;
            this.username = username;
            this.modelType = modelType;
        }

        @Override
        public void validateConfigsBeforeLoad(
            GraphProjectConfig graphProjectConfig,
            TRAIN_CONFIG config
        ) {
            modelCatalog.verifyModelCanBeStored(
                username,
                config.modelName(),
                modelType
            );
        }
    }

    // FIXME replace this with MLTrainResult (duplicate?)
    @SuppressWarnings("unused")
    public static class TrainResult {

        public final Map<String, Object> modelInfo;
        public final Map<String, Object> configuration;
        public final long trainMillis;

        public <TRAIN_RESULT, TRAIN_CONFIG extends ModelConfig & AlgoBaseConfig, TRAIN_INFO extends CustomInfo> TrainResult(
            Model<TRAIN_RESULT, TRAIN_CONFIG, TRAIN_INFO> trainedModel,
            long trainMillis,
            long nodeCount,
            long relationshipCount
        ) {
            TRAIN_CONFIG trainConfig = trainedModel.trainConfig();

            this.modelInfo = new HashMap<>();
            modelInfo.put(MODEL_NAME_KEY, trainedModel.name());
            modelInfo.put(MODEL_TYPE_KEY, trainedModel.algoType());
            modelInfo.putAll(trainedModel.customInfo().toMap());

            this.configuration = trainConfig.toMap();
            this.trainMillis = trainMillis;
        }
    }
}
