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
import org.neo4j.gds.applications.algorithms.machinery.GraphStoreService;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.core.model.Model;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;
import org.neo4j.gds.ml.pipeline.AutoTuningConfig;
import org.neo4j.gds.ml.pipeline.NodePropertyStepFactory;
import org.neo4j.gds.ml.pipeline.PipelineCatalog;
import org.neo4j.gds.ml.pipeline.TrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

class PipelineApplications {
    private final Log log;
    private final GraphStoreService gss;
    private final ModelCatalog modelCatalog;
    private final PipelineRepository pipelineRepository;

    private final CloseableResourceRegistry closeableResourceRegistry;
    private final DatabaseId databaseId;
    private final DependencyResolver dependencyResolver;
    private final Metrics metrics;
    private final NodeLookup nodeLookup;
    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationMonitor terminationMonitor;
    private final User user;
    private final UserLogRegistryFactory userLogRegistryFactory;

    private final PipelineConfigurationParser pipelineConfigurationParser;
    private final ProgressTrackerCreator progressTrackerCreator;

    private final NodeClassificationPredictPipelineEstimator nodeClassificationPredictPipelineEstimator;
    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    PipelineApplications(
        Log log,
        GraphStoreService graphStoreService,
        ModelCatalog modelCatalog,
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
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        PipelineConfigurationParser pipelineConfigurationParser,
        ProgressTrackerCreator progressTrackerCreator,
        NodeClassificationPredictPipelineEstimator nodeClassificationPredictPipelineEstimator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.log = log;
        this.gss = graphStoreService;
        this.modelCatalog = modelCatalog;
        this.pipelineRepository = pipelineRepository;
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.databaseId = databaseId;
        this.dependencyResolver = dependencyResolver;
        this.metrics = metrics;
        this.nodeLookup = nodeLookup;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
        this.procedureReturnColumns = procedureReturnColumns;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationMonitor = terminationMonitor;
        this.user = user;
        this.userLogRegistryFactory = userLogRegistryFactory;
        this.pipelineConfigurationParser = pipelineConfigurationParser;
        this.progressTrackerCreator = progressTrackerCreator;
        this.nodeClassificationPredictPipelineEstimator = nodeClassificationPredictPipelineEstimator;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    static PipelineApplications create(
        Log log,
        ModelCatalog modelCatalog,
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
        User user,
        UserLogRegistryFactory userLogRegistryFactory,
        PipelineConfigurationParser pipelineConfigurationParser,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        var graphStoreService = new GraphStoreService(log);

        var nodeClassificationPredictPipelineEstimator = new NodeClassificationPredictPipelineEstimator(
            modelCatalog,
            algorithmsProcedureFacade
        );

        return new PipelineApplications(
            log,
            graphStoreService,
            modelCatalog,
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
            user,
            userLogRegistryFactory,
            pipelineConfigurationParser,
            progressTrackerCreator,
            nodeClassificationPredictPipelineEstimator,
            algorithmsProcedureFacade,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate
        );
    }

    NodeClassificationTrainingPipeline addNodeProperty(
        PipelineName pipelineName,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(user, pipelineName);

        var nodePropertyStep = NodePropertyStepFactory.createNodePropertyStep(taskName, procedureConfig);

        pipeline.addNodePropertyStep(nodePropertyStep);

        return pipeline;
    }

    NodeClassificationTrainingPipeline addTrainerConfiguration(
        PipelineName pipelineName,
        TunableTrainerConfig configuration
    ) {
        return configure(pipelineName, pipeline -> pipeline.addTrainerConfig(configuration));
    }

    NodeClassificationTrainingPipeline configureAutoTuning(PipelineName pipelineName, AutoTuningConfig configuration) {
        return configure(pipelineName, pipeline -> pipeline.setAutoTuningConfig(configuration));
    }

    NodeClassificationTrainingPipeline configureSplit(
        PipelineName pipelineName, NodePropertyPredictionSplitConfig configuration
    ) {
        return configure(pipelineName, pipeline -> pipeline.setSplitConfig(configuration));
    }

    NodeClassificationTrainingPipeline createNodeClassificationTrainingPipeline(PipelineName pipelineName) {
        return pipelineRepository.createNodeClassificationTrainingPipeline(user, pipelineName);
    }

    /**
     * Straight delegation, the store will throw an exception
     */
    TrainingPipeline<?> dropAcceptingFailure(PipelineName pipelineName) {
        return pipelineRepository.drop(user, pipelineName);
    }

    /**
     * Pre-check for existence, return null
     */
    TrainingPipeline<?> dropSilencingFailure(PipelineName pipelineName) {
        if (!pipelineRepository.exists(user, pipelineName)) return null;

        return pipelineRepository.drop(user, pipelineName);
    }

    /**
     * @return the pipeline type, if the pipeline exists. Bit of an overload :)
     */
    Optional<String> exists(PipelineName pipelineName) {
        var pipelineExists = pipelineRepository.exists(user, pipelineName);

        if (!pipelineExists) return Optional.empty();

        var pipelineType = pipelineRepository.getType(user, pipelineName);

        return Optional.of(pipelineType);
    }

    Stream<PipelineCatalog.PipelineCatalogEntry> getAll() {
        return pipelineRepository.getAll(user);
    }

    Optional<TrainingPipeline<?>> getSingle(PipelineName pipelineName) {
        return pipelineRepository.getSingle(user, pipelineName);
    }

    NodeClassificationTrainingPipeline selectFeatures(
        PipelineName pipelineName,
        Iterable<NodeFeatureStep> nodeFeatureSteps
    ) {
        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(user, pipelineName);

        for (NodeFeatureStep nodeFeatureStep : nodeFeatureSteps) {
            pipeline.addFeatureStep(nodeFeatureStep);
        }

        return pipeline;
    }

    private NodeClassificationTrainingPipeline configure(
        PipelineName pipelineName,
        Consumer<NodeClassificationTrainingPipeline> configurationAction
    ) {
        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(
            user,
            pipelineName
        );

        configurationAction.accept(pipeline);

        return pipeline;
    }

    PredictMutateResult nodeClassificationMutate(GraphName graphName, Map<String, Object> rawConfiguration) {
        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictPipelineMutateConfig(rawConfiguration);

        var label = new StandardLabel("NodeClassificationPredictPipelineMutate");

        var computation = NodeClassificationPredictPipelineMutateComputation.create(
            log,
            modelCatalog,
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
            user,
            userLogRegistryFactory,
            progressTrackerCreator,
            algorithmsProcedureFacade,
            configuration,
            label
        );

        var mutateStep = new NodeClassificationPredictPipelineMutateStep(gss, configuration);

        var resultBuilder = new NodeClassificationPredictPipelineMutateResultBuilder(configuration);

        return algorithmProcessingTemplate.processAlgorithmForMutate(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            () -> nodeClassificationMutateMemoryEstimation(configuration),
            computation,
            mutateStep,
            resultBuilder
        );
    }

    MemoryEstimateResult nodeClassificationMutateEstimate(
        Object graphNameOrConfiguration,
        NodeClassificationPredictPipelineMutateConfig configuration
    ) {
        var estimate = nodeClassificationMutateMemoryEstimation(configuration);

        var memoryEstimation = MemoryEstimations.builder("Node Classification Predict Pipeline Executor")
            .add("Pipeline executor", estimate)
            .build();

        return algorithmEstimationTemplate.estimate(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    private MemoryEstimation nodeClassificationMutateMemoryEstimation(NodeClassificationPredictPipelineBaseConfig configuration) {
        var model = getTrainedNCPipelineModel(configuration.modelName(), configuration.username());

        return nodeClassificationPredictPipelineEstimator.estimate(model, configuration);
    }

    private Model<Classifier.ClassifierData, NodeClassificationPipelineTrainConfig, NodeClassificationPipelineModelInfo> getTrainedNCPipelineModel(
        String modelName,
        String username
    ) {
        return modelCatalog.get(
            username,
            modelName,
            Classifier.ClassifierData.class,
            NodeClassificationPipelineTrainConfig.class,
            NodeClassificationPipelineModelInfo.class
        );
    }
}
