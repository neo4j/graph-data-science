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
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class NodeClassificationFacade {
    private final NodeClassificationPredictConfigPreProcessor nodeClassificationPredictConfigPreProcessor;
    private final PipelineConfigurationParser pipelineConfigurationParser;
    private final PipelineApplications pipelineApplications;

    NodeClassificationFacade(
        NodeClassificationPredictConfigPreProcessor nodeClassificationPredictConfigPreProcessor,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications
    ) {
        this.nodeClassificationPredictConfigPreProcessor = nodeClassificationPredictConfigPreProcessor;
        this.pipelineConfigurationParser = pipelineConfigurationParser;
        this.pipelineApplications = pipelineApplications;
    }

    static NodeClassificationFacade create(
        ModelCatalog modelCatalog,
        User user,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications
    ) {
        var nodeClassificationPredictConfigPreProcessor = new NodeClassificationPredictConfigPreProcessor(
            modelCatalog,
            user
        );

        return new NodeClassificationFacade(
            nodeClassificationPredictConfigPreProcessor,
            pipelineConfigurationParser,
            pipelineApplications
        );
    }

    public Stream<NodePipelineInfoResult> addLogisticRegression(
        String pipelineName,
        Map<String, Object> configuration
    ) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseLogisticRegressionTrainerConfig(configuration),
            pipelineApplications::addTrainerConfiguration
        );
    }

    public Stream<NodePipelineInfoResult> addMLP(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseMLPClassifierTrainConfig(configuration),
            pipelineApplications::addTrainerConfiguration
        );
    }

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

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<NodePipelineInfoResult> addRandomForest(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseRandomForestClassifierTrainerConfig(
                configuration),
            pipelineApplications::addTrainerConfiguration
        );
    }

    public Stream<NodePipelineInfoResult> configureAutoTuning(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseAutoTuningConfig(configuration),
            pipelineApplications::configureAutoTuning
        );
    }

    public Stream<NodePipelineInfoResult> configureSplit(String pipelineName, Map<String, Object> configuration) {
        return configure(
            pipelineName,
            () -> pipelineConfigurationParser.parseNodePropertyPredictionSplitConfig(
                configuration),
            pipelineApplications::configureSplit
        );
    }

    public Stream<NodePipelineInfoResult> createPipeline(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.createNodeClassificationTrainingPipeline(
            pipelineName);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<PredictMutateResult> mutate(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(
            configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeClassificationPredictMutate(
            graphName,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> mutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(
            rawConfiguration);

        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictMutateConfig(
            rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<NodePipelineInfoResult> selectFeatures(String pipelineNameAsString, Object nodeFeatureStepsAsObject) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var nodeFeatureSteps = parseNodeProperties(nodeFeatureStepsAsObject);

        var pipeline = pipelineApplications.selectFeatures(pipelineName, nodeFeatureSteps);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<NodeClassificationStreamResult> stream(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(
            configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeClassificationPredictStream(graphName, configuration);
    }

    public Stream<MemoryEstimateResult> streamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(
            rawConfiguration);

        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictStreamConfig(
            rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<NodeClassificationPipelineTrainResult> train(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeClassificationTrain(graphName, configuration);
    }

    public Stream<MemoryEstimateResult> trainEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        var configuration = pipelineConfigurationParser.parseNodeClassificationTrainConfig(
            rawConfiguration);

        var result = pipelineApplications.nodeClassificationTrainEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<WriteResult> write(String graphNameAsString, Map<String, Object> configuration) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(
            configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeClassificationPredictWrite(
            graphName,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> writeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictWriteConfig(
            rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    private <CONFIGURATION> Stream<NodePipelineInfoResult> configure(
        String pipelineNameAsString,
        Supplier<CONFIGURATION> configurationSupplier,
        BiFunction<PipelineName, CONFIGURATION, NodeClassificationTrainingPipeline> configurationAction
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var configuration = configurationSupplier.get();

        var pipeline = configurationAction.apply(pipelineName, configuration);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    private List<NodeFeatureStep> parseNodeProperties(Object nodeProperties) {
        if (nodeProperties instanceof String) return List.of(NodeFeatureStep.of((String) nodeProperties));

        if (nodeProperties instanceof List) {
            //noinspection rawtypes
            var propertiesList = (List) nodeProperties;

            var nodeFeatureSteps = new ArrayList<NodeFeatureStep>(propertiesList.size());

            for (Object o : propertiesList) {
                if (o instanceof String)
                    nodeFeatureSteps.add(NodeFeatureStep.of((String) o));
                else
                    throw new IllegalArgumentException("The list `nodeProperties` is required to contain only strings.");
            }

            return nodeFeatureSteps;
        }

        throw new IllegalArgumentException("The value of `nodeProperties` is required to be a list of strings.");
    }
}
