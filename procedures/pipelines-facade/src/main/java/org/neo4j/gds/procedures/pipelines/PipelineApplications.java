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
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.GraphStoreService;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.NodePropertyWriter;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.StandardLabel;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
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
import org.neo4j.gds.ml.pipeline.linkPipeline.LinkPredictionTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureStep;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodePropertyPredictionSplitConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.NodeClassificationTrainingPipeline;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationModelResult;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineModelInfo;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.classification.train.NodeClassificationTrain;
import org.neo4j.gds.model.ModelConfig;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.termination.TerminationMonitor;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.config.RelationshipWeightConfig.RELATIONSHIP_WEIGHT_PROPERTY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class PipelineApplications {
    private final Log log;
    private final GraphStoreService graphStoreService;
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
    private final NodeClassificationTrainSideEffectsFactory nodeClassificationTrainSideEffectsFactory;

    private final NodePropertyWriter nodePropertyWriter;

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
        NodeClassificationTrainSideEffectsFactory nodeClassificationTrainSideEffectsFactory,
        NodePropertyWriter nodePropertyWriter,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        AlgorithmEstimationTemplate algorithmEstimationTemplate,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.log = log;
        this.graphStoreService = graphStoreService;
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
        this.nodeClassificationTrainSideEffectsFactory = nodeClassificationTrainSideEffectsFactory;
        this.nodePropertyWriter = nodePropertyWriter;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
    }

    static PipelineApplications create(
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

        var nodeClassificationTrainSideEffectsFactory = new NodeClassificationTrainSideEffectsFactory(
            log,
            modelCatalog,
            modelRepository
        );

        var nodePropertyWriter = new NodePropertyWriter(
            log,
            nodePropertyExporterBuilder,
            taskRegistryFactory,
            terminationFlag
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
            nodeClassificationTrainSideEffectsFactory,
            nodePropertyWriter,
            algorithmsProcedureFacade,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate
        );
    }

    LinkPredictionTrainingPipeline addNodePropertyToLinkPredictionPipeline(
        PipelineName pipelineName,
        String taskName,
        Map<String, Object> procedureConfig
    ) {
        var pipeline = pipelineRepository.getLinkPredictionTrainingPipeline(user, pipelineName);
        validateRelationshipProperty(pipeline, procedureConfig);

        var nodePropertyStep = NodePropertyStepFactory.createNodePropertyStep(taskName, procedureConfig);

        pipeline.addNodePropertyStep(nodePropertyStep);

        return pipeline;
    }

    NodeClassificationTrainingPipeline addNodePropertyToNodeClassificationPipeline(
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

    MemoryEstimateResult nodeClassificationPredictEstimate(
        Object graphNameOrConfiguration,
        NodeClassificationPredictPipelineBaseConfig configuration
    ) {
        var estimate = nodeClassificationPredictMemoryEstimation(configuration);

        var memoryEstimation = MemoryEstimations.builder("Node Classification Predict Pipeline Executor")
            .add("Pipeline executor", estimate)
            .build();

        return algorithmEstimationTemplate.estimate(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    PredictMutateResult nodeClassificationPredictMutate(GraphName graphName, Map<String, Object> rawConfiguration) {
        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictMutateConfig(rawConfiguration);
        var label = new StandardLabel("NodeClassificationPredictPipelineMutate");
        var computation = constructPredictComputation(configuration, label);
        var mutateStep = new NodeClassificationPredictPipelineMutateStep(graphStoreService, configuration);
        var resultBuilder = new NodeClassificationPredictPipelineMutateResultBuilder(configuration);

        return algorithmProcessingTemplate.processAlgorithmForMutate(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            () -> nodeClassificationPredictMemoryEstimation(configuration),
            computation,
            mutateStep,
            resultBuilder
        );
    }

    Stream<NodeClassificationStreamResult> nodeClassificationPredictStream(
        GraphName graphName,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictStreamConfig(rawConfiguration);
        var label = new StandardLabel("NodeClassificationPredictPipelineStream");
        var computation = constructPredictComputation(configuration, label);
        var resultBuilder = new NodeClassificationPredictPipelineStreamResultBuilder(configuration);

        return algorithmProcessingTemplate.processAlgorithmForStream(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            () -> nodeClassificationPredictMemoryEstimation(configuration),
            computation,
            resultBuilder
        );
    }

    WriteResult nodeClassificationPredictWrite(GraphName graphName, Map<String, Object> rawConfiguration) {
        var configuration = pipelineConfigurationParser.parseNodeClassificationPredictWriteConfig(rawConfiguration);
        var label = new StandardLabel("NodeClassificationPredictPipelineWrite");
        var computation = constructPredictComputation(configuration, label);
        var writeStep = new NodeClassificationPredictPipelineWriteStep(nodePropertyWriter, configuration);
        var resultBuilder = new NodeClassificationPredictPipelineWriteResultBuilder(configuration);

        return algorithmProcessingTemplate.processAlgorithmForWrite(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            () -> nodeClassificationPredictMemoryEstimation(configuration),
            computation,
            writeStep,
            resultBuilder
        );
    }

    Stream<NodeClassificationPipelineTrainResult> nodeClassificationTrain(
        GraphName graphName,
        Map<String, Object> rawConfiguration
    ) {
        var configuration = pipelineConfigurationParser.parseNodeClassificationTrainConfig(rawConfiguration);

        ensureTrainingModelCanBeStored(configuration);

        var label = new StandardLabel("NodeClassificationPipelineTrain");
        var computation = constructTrainComputation(configuration);
        var sideEffects = nodeClassificationTrainSideEffectsFactory.create(configuration);
        var resultRenderer = new NodeClassificationTrainResultRenderer();

        return algorithmProcessingTemplate.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            label,
            () -> nodeClassificationTrainEstimation(configuration),
            computation,
            Optional.of(sideEffects),
            resultRenderer
        );
    }

    MemoryEstimateResult nodeClassificationTrainEstimate(
        Object graphNameOrConfiguration,
        NodeClassificationPipelineTrainConfig configuration
    ) {
        var memoryEstimation = nodeClassificationTrainEstimation(configuration);

        return algorithmEstimationTemplate.estimate(configuration, graphNameOrConfiguration, memoryEstimation);
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

    private NodeClassificationPredictComputation constructPredictComputation(
        NodeClassificationPredictPipelineBaseConfig configuration,
        Label label
    ) {
        return NodeClassificationPredictComputation.create(
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
    }

    private Computation<NodeClassificationModelResult> constructTrainComputation(
        NodeClassificationPipelineTrainConfig configuration
    ) {
        return NodeClassificationTrainComputation.create(
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
            configuration
        );
    }

    /**
     * We check up front that we can do the business logic around model storage.
     * That could/ should perhaps be built in so that I as the dev won't forget.
     * Not sure why it is like this.
     */
    private void ensureTrainingModelCanBeStored(ModelConfig configuration) {
        modelCatalog.verifyModelCanBeStored(
            user.getUsername(),
            configuration.modelName(),
            NodeClassificationTrainingPipeline.MODEL_TYPE
        );
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

    private MemoryEstimation nodeClassificationPredictMemoryEstimation(NodeClassificationPredictPipelineBaseConfig configuration) {
        var model = getTrainedNCPipelineModel(configuration.modelName(), configuration.username());

        return nodeClassificationPredictPipelineEstimator.estimate(model, configuration);
    }

    private MemoryEstimation nodeClassificationTrainEstimation(NodeClassificationPipelineTrainConfig configuration) {
        var specifiedUser = new User(configuration.username(), false);
        var pipelineName = PipelineName.parse(configuration.pipeline());

        var pipeline = pipelineRepository.getNodeClassificationTrainingPipeline(
            specifiedUser,
            pipelineName
        );

        var estimate = NodeClassificationTrain.estimate(
            pipeline,
            configuration,
            modelCatalog,
            algorithmsProcedureFacade
        );

        return MemoryEstimations.builder("Node Classification Train")
            .add(estimate)
            .build();
    }

    /**
     * check if adding would result in more than one relationshipWeightProperty
     */
    private void validateRelationshipProperty(
        LinkPredictionTrainingPipeline pipeline,
        Map<String, Object> procedureConfig
    ) {
        if (!procedureConfig.containsKey(RELATIONSHIP_WEIGHT_PROPERTY)) return;
        var maybeRelationshipProperty = pipeline.relationshipWeightProperty(modelCatalog, user.getUsername());
        if (maybeRelationshipProperty.isEmpty()) return;
        var relationshipProperty = maybeRelationshipProperty.get();
        var property = (String) procedureConfig.get(RELATIONSHIP_WEIGHT_PROPERTY);
        if (relationshipProperty.equals(property)) return;

        String tasks = pipeline.tasksByRelationshipProperty(modelCatalog, user.getUsername())
            .get(relationshipProperty)
            .stream()
            .map(s -> "`" + s + "`")
            .collect(Collectors.joining(", "));

        throw new IllegalArgumentException(formatWithLocale(
            "Node property steps added to a pipeline may not have different non-null values for `%s`. " +
                "Pipeline already contains tasks %s which use the value `%s`.",
            RELATIONSHIP_WEIGHT_PROPERTY,
            tasks,
            relationshipProperty
        ));
    }
}
