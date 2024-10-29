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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.pipeline.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.Map;
import java.util.stream.Stream;

public final class LocalNodeClassificationFacade implements NodeClassificationFacade {
    private final NodeFeatureStepsParser nodeFeatureStepsParser = new NodeFeatureStepsParser();

    private final Configurer configurer;
    private final NodeClassificationPredictConfigPreProcessor nodeClassificationPredictConfigPreProcessor;

    private final PipelineConfigurationParser pipelineConfigurationParser;
    private final PipelineApplications pipelineApplications;

    LocalNodeClassificationFacade(
        Configurer configurer,
        NodeClassificationPredictConfigPreProcessor nodeClassificationPredictConfigPreProcessor,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications
    ) {
        this.configurer = configurer;
        this.nodeClassificationPredictConfigPreProcessor = nodeClassificationPredictConfigPreProcessor;
        this.pipelineConfigurationParser = pipelineConfigurationParser;
        this.pipelineApplications = pipelineApplications;
    }

    static NodeClassificationFacade create(
        ModelCatalog modelCatalog,
        User user,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications,
        PipelineRepository pipelineRepository
    ) {
        var configurer = new Configurer(pipelineRepository, user);
        var nodeClassificationPredictConfigPreProcessor = new NodeClassificationPredictConfigPreProcessor(
            modelCatalog,
            user
        );

        return new LocalNodeClassificationFacade(
            configurer,
            nodeClassificationPredictConfigPreProcessor,
            pipelineConfigurationParser,
            pipelineApplications
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> addLogisticRegression(
        String pipelineName,
        Map<String, Object> configuration
    ) {
        return configurer.configureNodeClassificationTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseLogisticRegressionTrainerConfigForLinkPredictionOrNodeClassification(configuration),
            TrainingPipeline::addTrainerConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> addMLP(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeClassificationTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseMLPClassifierTrainConfig(configuration),
            TrainingPipeline::addTrainerConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> addNodeProperty(
        String pipelineNameAsString,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.addNodePropertyToNodeClassificationPipeline(
            pipelineName,
            taskName,
            procedureConfig
        );

        var result = NodePipelineInfoResultTransformer.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    @Override
    public Stream<NodePipelineInfoResult> addRandomForest(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeClassificationTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseRandomForestClassifierTrainerConfigForLinkPredictionOrNodeClassification(configuration),
            TrainingPipeline::addTrainerConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> configureAutoTuning(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeClassificationTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseAutoTuningConfig(configuration),
            TrainingPipeline::setAutoTuningConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> configureSplit(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeClassificationTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseNodePropertyPredictionSplitConfig(configuration),
            NodeClassificationTrainingPipeline::setSplitConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> createPipeline(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.createNodeClassificationTrainingPipeline(pipelineName);

        var result = NodePipelineInfoResultTransformer.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    @Override
    public Stream<PredictMutateResult> mutate(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeClassificationPredictMutate(
            graphName,
            configuration
        );

        return Stream.of(result);
    }

    @Override
    public Stream<MemoryEstimateResult> mutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(rawConfiguration);

        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictMutateConfig(
            rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    @Override
    public Stream<NodePipelineInfoResult> selectFeatures(String pipelineNameAsString, Object nodeFeatureStepsAsObject) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var nodeFeatureSteps = nodeFeatureStepsParser.parse(nodeFeatureStepsAsObject, "nodeProperties");

        var pipeline = pipelineApplications.selectFeaturesForClassification(pipelineName, nodeFeatureSteps);

        var result = NodePipelineInfoResultTransformer.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    @Override
    public Stream<NodeClassificationStreamResult> stream(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeClassificationPredictStream(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(rawConfiguration);

        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictStreamConfig(
            rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    @Override
    public Stream<NodeClassificationPipelineTrainResult> train(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeClassificationTrain(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> trainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        var configuration = pipelineConfigurationParser.parseNodeClassificationTrainConfig(rawConfiguration);

        var result = pipelineApplications.nodeClassificationTrainEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    @Override
    public Stream<WriteResult> write(String graphNameAsString, Map<String, Object> configuration) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeClassificationPredictWrite(
            graphName,
            configuration
        );

        return Stream.of(result);
    }

    @Override
    public Stream<MemoryEstimateResult> writeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictWriteConfig(rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }
}
