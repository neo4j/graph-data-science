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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.pipeline.PipelineCompanion;
import org.neo4j.gds.ml.pipeline.nodePipeline.NodeFeatureProducer;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionPipelineTrainConfig;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrain;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainAlgorithm;
import org.neo4j.gds.ml.pipeline.nodePipeline.regression.NodeRegressionTrainResult;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationMonitor;

final class NodeRegressionTrainComputation implements Computation<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult> {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final Log log;
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
    private final UserLogRegistryFactory userLogRegistryFactory;
    private final ProgressTrackerCreator progressTrackerCreator;
    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;
    private final NodeRegressionPipelineTrainConfig configuration;

    private NodeRegressionTrainComputation(
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
        UserLogRegistryFactory userLogRegistryFactory,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        NodeRegressionPipelineTrainConfig configuration
    ) {
        this.log = log;
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
        this.userLogRegistryFactory = userLogRegistryFactory;
        this.progressTrackerCreator = progressTrackerCreator;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.configuration = configuration;
    }

    static Computation<NodeRegressionTrainResult.NodeRegressionTrainPipelineResult> create(
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
        UserLogRegistryFactory userLogRegistryFactory,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        NodeRegressionPipelineTrainConfig configuration
    ) {
        return new NodeRegressionTrainComputation(
            log,
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
            userLogRegistryFactory,
            progressTrackerCreator,
            algorithmsProcedureFacade,
            configuration
        );
    }

    @Override
    public NodeRegressionTrainResult.NodeRegressionTrainPipelineResult compute(Graph graph, GraphStore graphStore) {
        var user = new User(configuration.username(), false);
        var pipelineName = PipelineName.parse(configuration.pipeline());
        var pipeline = pipelineRepository.getNodeRegressionTrainingPipeline(
            user,
            pipelineName
        );

        PipelineCompanion.validateMainMetric(pipeline, configuration.metrics().get(0).toString());

        var executionContext = ImmutableExecutionContext.builder()
            .algorithmsProcedureFacade(algorithmsProcedureFacade)
            .closeableResourceRegistry(closeableResourceRegistry)
            .databaseId(databaseId)
            .dependencyResolver(dependencyResolver)
            .isGdsAdmin(user.isAdmin())
            .log(log)
            .metrics(metrics)
            .modelCatalog(modelCatalog)
            .nodeLookup(nodeLookup)
            .nodePropertyExporterBuilder(nodePropertyExporterBuilder)
            .relationshipExporterBuilder(relationshipExporterBuilder)
            .returnColumns(procedureReturnColumns)
            .taskRegistryFactory(taskRegistryFactory)
            .terminationMonitor(terminationMonitor)
            .userLogRegistryFactory(userLogRegistryFactory)
            .username(user.getUsername())
            .build();

        var task = NodeRegressionTrain.progressTask(pipeline, graphStore.nodeCount());
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var nodeFeatureProducer = NodeFeatureProducer.create(
            graphStore,
            configuration,
            executionContext,
            progressTracker
        );

        nodeFeatureProducer.validateNodePropertyStepsContextConfigs(pipeline.nodePropertySteps());

        var pipelineTrainer = NodeRegressionTrain.create(
            graphStore,
            pipeline,
            configuration,
            nodeFeatureProducer,
            progressTracker
        );

        var algorithm = new NodeRegressionTrainAlgorithm(
            pipelineTrainer,
            pipeline,
            graphStore,
            configuration,
            progressTracker
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }
}
