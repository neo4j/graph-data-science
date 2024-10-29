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
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.ml.pipeline.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainingPipeline;

import java.util.Map;
import java.util.stream.Stream;

final class LocalNodeRegressionFacade implements NodeRegressionFacade {
    private final NodeFeatureStepsParser nodeFeatureStepsParser = new NodeFeatureStepsParser();

    private final Configurer configurer;
    private final NodeRegressionPredictConfigPreProcessor nodeRegressionPredictConfigPreProcessor;

    private final PipelineConfigurationParser pipelineConfigurationParser;
    private final PipelineApplications pipelineApplications;

    private LocalNodeRegressionFacade(
        Configurer configurer,
        NodeRegressionPredictConfigPreProcessor nodeRegressionPredictConfigPreProcessor,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications
    ) {
        this.configurer = configurer;
        this.nodeRegressionPredictConfigPreProcessor = nodeRegressionPredictConfigPreProcessor;
        this.pipelineConfigurationParser = pipelineConfigurationParser;
        this.pipelineApplications = pipelineApplications;
    }

    static NodeRegressionFacade create(
        ModelCatalog modelCatalog,
        User user,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications,
        PipelineRepository pipelineRepository
    ) {
        var configurer = new Configurer(pipelineRepository, user);
        var nodeRegressionPredictConfigPreProcessor = new NodeRegressionPredictConfigPreProcessor(modelCatalog, user);

        return new LocalNodeRegressionFacade(
            configurer,
            nodeRegressionPredictConfigPreProcessor,
            pipelineConfigurationParser,
            pipelineApplications
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> addLogisticRegression(
        String pipelineName,
        Map<String, Object> configuration
    ) {
        return configurer.configureNodeRegressionTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseLogisticRegressionTrainerConfigForNodeRegression(configuration),
            TrainingPipeline::addTrainerConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> addNodeProperty(
        String pipelineNameAsString, String taskName, Map<String, Object> procedureConfig
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.addNodePropertyToNodeRegressionPipeline(
            pipelineName,
            taskName,
            procedureConfig
        );

        var result = NodePipelineInfoResultTransformer.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    @Override
    public Stream<NodePipelineInfoResult> addRandomForest(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeRegressionTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseRandomForestClassifierTrainerConfigForNodeRegression(configuration),
            TrainingPipeline::addTrainerConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> configureAutoTuning(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeRegressionTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseAutoTuningConfig(configuration),
            TrainingPipeline::setAutoTuningConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> configureSplit(String pipelineName, Map<String, Object> configuration) {
        return configurer.configureNodeRegressionTrainingPipeline(
            pipelineName,
            () -> pipelineConfigurationParser.parseNodePropertyPredictionSplitConfig(configuration),
            NodeRegressionTrainingPipeline::setSplitConfig
        );
    }

    @Override
    public Stream<NodePipelineInfoResult> createPipeline(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.createNodeRegressionTrainingPipeline(pipelineName);

        var result = NodePipelineInfoResultTransformer.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    @Override
    public Stream<PredictMutateResult> mutate(String graphNameAsString, Map<String, Object> configuration) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeRegressionPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeRegressionPredictMutate(graphName, configuration);

        return Stream.of(result);
    }

    @Override
    public Stream<NodeRegressionStreamResult> stream(String graphNameAsString, Map<String, Object> configuration) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeRegressionPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeRegressionPredictStream(graphName, configuration);
    }

    @Override
    public Stream<NodePipelineInfoResult> selectFeatures(String pipelineNameAsString, Object nodeFeatureStepsAsObject) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var nodeFeatureSteps = nodeFeatureStepsParser.parse(nodeFeatureStepsAsObject, "featureProperties");

        var pipeline = pipelineApplications.selectFeaturesForRegression(pipelineName, nodeFeatureSteps);

        var result = NodePipelineInfoResultTransformer.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    @Override
    public Stream<NodeRegressionPipelineTrainResult> train(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeRegressionTrain(graphName, configuration);
    }
}
