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
package org.neo4j.gds.procedures.pipelines;

import org.neo4j.gds.api.User;
import org.neo4j.gds.core.ConfigKeyValidation;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.ml.api.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.models.logisticregression.LogisticRegressionTrainConfig;
import org.neo4j.gds.ml.models.mlp.MLPClassifierTrainConfig;
import org.neo4j.gds.ml.models.randomforest.RandomForestClassifierTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.LinkFeatureStepConfiguration;
import org.neo4j.gds.ml.pipeline.linkPipeline.linkfunctions.LinkFeatureStepConfigurationImpl;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

class PipelineConfigurationParser {
    private final User user;

    PipelineConfigurationParser(User user) {
        this.user = user;
    }

    AutoTuningConfig parseAutoTuningConfig(Map<String, Object> rawConfiguration) {
        return parseConfigurationWithValidation(
            rawConfiguration,
            AutoTuningConfig::of,
            AutoTuningConfig::configKeys
        );
    }

    LinkFeatureStepConfiguration parseLinkFeatureStepConfiguration(Map<String, Object> configuration) {
        var wrapper = CypherMapWrapper.create(configuration);

        return new LinkFeatureStepConfigurationImpl(wrapper);
    }

    LinkPredictionPredictPipelineMutateConfig parseLinkPredictionPredictPipelineMutateConfig(Map<String, Object> configuration) {
        return parseConfiguration(LinkPredictionPredictPipelineMutateConfig::of, configuration);
    }

    LinkPredictionSplitConfig parseLinkPredictionSplitConfig(Map<String, Object> rawConfiguration) {
        return parseConfigurationWithValidation(
            rawConfiguration,
            LinkPredictionSplitConfig::of,
            LinkPredictionSplitConfig::configKeys
        );
    }

    TunableTrainerConfig parseLogisticRegressionTrainerConfig(Map<String, Object> configuration) {
        return parseTrainerConfiguration(
            configuration,
            LogisticRegressionTrainConfig.DEFAULT.configKeys(),
            TrainingMethod.LogisticRegression
        );
    }

    TunableTrainerConfig parseMLPClassifierTrainConfig(Map<String, Object> configuration) {
        return parseTrainerConfiguration(
            configuration,
            MLPClassifierTrainConfig.DEFAULT.configKeys(),
            TrainingMethod.MLPClassification
        );
    }

    NodeClassificationPipelineTrainConfig parseNodeClassificationTrainConfig(Map<String, Object> configuration) {
        return parseConfiguration(NodeClassificationPipelineTrainConfig::of, configuration);
    }

    NodeClassificationPredictPipelineMutateConfig parseNodeClassificationPredictMutateConfig(Map<String, Object> configuration) {
        return parseConfiguration(NodeClassificationPredictPipelineMutateConfig::of, configuration);
    }

    NodeClassificationPredictPipelineStreamConfig parseNodeClassificationPredictStreamConfig(Map<String, Object> configuration) {
        return parseConfiguration(NodeClassificationPredictPipelineStreamConfig::of, configuration);
    }

    NodeClassificationPredictPipelineWriteConfig parseNodeClassificationPredictWriteConfig(Map<String, Object> configuration) {
        return parseConfiguration(NodeClassificationPredictPipelineWriteConfig::of, configuration);
    }

    NodePropertyPredictionSplitConfig parseNodePropertyPredictionSplitConfig(Map<String, Object> rawConfiguration) {
        return parseConfigurationWithValidation(
            rawConfiguration,
            NodePropertyPredictionSplitConfig::of,
            NodePropertyPredictionSplitConfig::configKeys
        );
    }

    TunableTrainerConfig parseRandomForestClassifierTrainerConfig(Map<String, Object> configuration) {
        return parseTrainerConfiguration(
            configuration,
            RandomForestClassifierTrainerConfig.DEFAULT.configKeys(),
            TrainingMethod.RandomForestClassification
        );
    }

    private <CONFIGURATION> CONFIGURATION parseConfigurationWithValidation(
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> parser,
        Function<CONFIGURATION, Collection<String>> configurationKeyAccessor
    ) {
        var cypherConfig = CypherMapWrapper.create(rawConfiguration);

        var configuration = parser.apply(cypherConfig);

        var configurationKeys = configurationKeyAccessor.apply(configuration);

        cypherConfig.requireOnlyKeysFrom(configurationKeys);

        return configuration;
    }

    /**
     * Dumb scaffolding
     */
    private <CONFIGURATION> CONFIGURATION parseConfiguration(
        BiFunction<String, CypherMapWrapper, CONFIGURATION> parser,
        Map<String, Object> configuration
    ) {
        var wrapper = CypherMapWrapper.create(configuration);

        return parser.apply(user.getUsername(), wrapper);
    }

    private TunableTrainerConfig parseTrainerConfiguration(
        Map<String, Object> configuration,
        Collection<String> allowedKeys,
        TrainingMethod trainingMethod
    ) {
        ConfigKeyValidation.requireOnlyKeysFrom(allowedKeys, configuration.keySet());

        return TunableTrainerConfig.of(configuration, trainingMethod);
    }
}
