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

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.pipeline.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class PipelinesProcedureFacade {
    public static final String NO_VALUE = "__NO_VALUE";

    private final NodeClassificationPredictConfigPreProcessor nodeClassificationPredictConfigPreProcessor;
    private final PipelineConfigurationParser pipelineConfigurationParser;

    private final PipelineApplications pipelineApplications;

    private final LinkPredictionFacade linkPredictionFacade;

    PipelinesProcedureFacade(
        NodeClassificationPredictConfigPreProcessor nodeClassificationPredictConfigPreProcessor,
        PipelineConfigurationParser pipelineConfigurationParser,
        PipelineApplications pipelineApplications,
        LinkPredictionFacade linkPredictionFacade
    ) {
        this.nodeClassificationPredictConfigPreProcessor = nodeClassificationPredictConfigPreProcessor;
        this.pipelineConfigurationParser = pipelineConfigurationParser;
        this.pipelineApplications = pipelineApplications;
        this.linkPredictionFacade = linkPredictionFacade;
    }

    public static PipelinesProcedureFacade create(
        Log log,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        PipelineRepository pipelineRepository,
        CloseableResourceRegistry closeableResourceRegistry,
        DatabaseId databaseId,
        DependencyResolver dependencyResolver,
        Metrics metrics,
        NodeLookup nodeLookup,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        ProcedureReturnColumns procedureReturnColumns,
        RelationshipExporterBuilder relationshipExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationMonitor terminationMonitor,
        TerminationFlag terminationFlag,
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        var nodeClassificationPredictConfigPreProcessor = new NodeClassificationPredictConfigPreProcessor(
            modelCatalog,
            user
        );

        var pipelineConfigurationParser = new PipelineConfigurationParser(user);

        var pipelineApplications = PipelineApplications.create(
            log,
            modelCatalog,
            modelRepository,
            pipelineRepository,
            closeableResourceRegistry,
            databaseId,
            dependencyResolver,
            metrics,
            nodeLookup,
            nodePropertyExporterBuilder,
            procedureReturnColumns,
            relationshipExporterBuilder,
            taskRegistryFactory,
            terminationMonitor,
            terminationFlag,
            user,
            userLogRegistryFactory,
            pipelineConfigurationParser,
            progressTrackerCreator,
            algorithmsProcedureFacade,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate
        );

        var linkPredictionFacade = new LinkPredictionFacade(pipelineApplications);

        return new PipelinesProcedureFacade(
            nodeClassificationPredictConfigPreProcessor,
            pipelineConfigurationParser,
            pipelineApplications,
            linkPredictionFacade
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
            () -> pipelineConfigurationParser.parseRandomForestClassifierTrainerConfig(configuration),
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
            () -> pipelineConfigurationParser.parseNodePropertyPredictionSplitConfig(configuration),
            pipelineApplications::configureSplit
        );
    }

    public Stream<NodePipelineInfoResult> createPipeline(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipeline = pipelineApplications.createNodeClassificationTrainingPipeline(pipelineName);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

        return Stream.of(result);
    }

    public Stream<PipelineCatalogResult> drop(
        String pipelineNameAsString,
        boolean failIfMissing
    ) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        if (failIfMissing) {
            var result = pipelineApplications.dropAcceptingFailure(pipelineName);

            return Stream.of(PipelineCatalogResult.create(result, pipelineName.value));
        }

        var result = pipelineApplications.dropSilencingFailure(pipelineName);

        return Stream.ofNullable(result).map(pipeline -> PipelineCatalogResult.create(pipeline, pipelineName.value));
    }

    public Stream<PipelineExistsResult> exists(String pipelineNameAsString) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var pipelineType = pipelineApplications.exists(pipelineName);

        if (pipelineType.isEmpty()) return Stream.of(PipelineExistsResult.empty(pipelineName));

        var result = new PipelineExistsResult(pipelineName.value, pipelineType.get(), true);

        return Stream.of(result);
    }

    public Stream<PipelineCatalogResult> list(String pipelineNameAsString) {
        if (pipelineNameAsString == null || pipelineNameAsString.equals(NO_VALUE)) {
            var pipelineEntries = pipelineApplications.getAll();

            return pipelineEntries.map(
                entry -> PipelineCatalogResult.create(
                    entry.pipeline(),
                    entry.pipelineName()
                )
            );
        }

        var pipelineName = PipelineName.parse(pipelineNameAsString);

        Optional<TrainingPipeline<?>> pipeline = pipelineApplications.getSingle(pipelineName);

        if (pipeline.isEmpty()) return Stream.empty();

        var result = PipelineCatalogResult.create(pipeline.get(), pipelineName.value);

        return Stream.of(result);
    }

    public Stream<PredictMutateResult> nodeClassificationMutate(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeClassificationPredictMutate(graphName, configuration);

        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> nodeClassificationMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(rawConfiguration);

        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictMutateConfig(rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<NodeClassificationStreamResult> nodeClassificationStream(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeClassificationPredictStream(graphName, configuration);
    }

    public Stream<MemoryEstimateResult> nodeClassificationStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> rawConfiguration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameOrConfiguration, rawConfiguration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(rawConfiguration);

        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictStreamConfig(rawConfiguration);

        var result = pipelineApplications.nodeClassificationPredictEstimate(
            graphNameOrConfiguration,
            configuration
        );

        return Stream.of(result);
    }

    public Stream<NodeClassificationPipelineTrainResult> nodeClassificationTrain(
        String graphNameAsString, Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);

        var graphName = GraphName.parse(graphNameAsString);

        return pipelineApplications.nodeClassificationTrain(graphName, configuration);
    }

    public Stream<MemoryEstimateResult> nodeClassificationTrainEstimate(
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

    public Stream<WriteResult> nodeClassificationWrite(
        String graphNameAsString,
        Map<String, Object> configuration
    ) {
        PipelineCompanion.preparePipelineConfig(graphNameAsString, configuration);
        nodeClassificationPredictConfigPreProcessor.enhanceInputWithPipelineParameters(configuration);

        var graphName = GraphName.parse(graphNameAsString);

        var result = pipelineApplications.nodeClassificationPredictWrite(graphName, configuration);

        return Stream.of(result);
    }

    public Stream<MemoryEstimateResult> nodeClassificationWriteEstimate(
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

    public Stream<NodePipelineInfoResult> selectFeatures(String pipelineNameAsString, Object nodeFeatureStepsAsObject) {
        var pipelineName = PipelineName.parse(pipelineNameAsString);

        var nodeFeatureSteps = parseNodeProperties(nodeFeatureStepsAsObject);

        var pipeline = pipelineApplications.selectFeatures(pipelineName, nodeFeatureSteps);

        var result = NodePipelineInfoResult.create(pipelineName, pipeline);

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

    public LinkPredictionFacade linkPrediction() {
        return linkPredictionFacade;
    }
}
