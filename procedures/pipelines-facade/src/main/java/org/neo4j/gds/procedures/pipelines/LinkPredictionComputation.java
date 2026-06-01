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

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.domain.services.GloballyScopedDependencies;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.MemoryEstimationContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.linkmodels.LinkPredictionResult;
import org.neo4j.gds.ml.models.ClassifierFactory;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationMonitor;

final class LinkPredictionComputation implements Computation<LinkPredictionResult> {
    private final Log log;
    private final GloballyScopedDependencies globallyScopedDependencies;

    private final CloseableResourceRegistry closeableResourceRegistry;
    private final DatabaseId databaseId;
    private final MemoryEstimationContext memoryEstimationContext;
    private final Metrics metrics;
    private final NodePropertyExporterBuilder nodePropertyExporterBuilder;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final RequestCorrelationId requestCorrelationId;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationMonitor terminationMonitor;
    private final User user;

    private final ProgressTrackerCreator progressTrackerCreator;

    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;

    private final TrainedLPPipelineModel trainedLPPipelineModel;
    private final LinkPredictionPredictPipelineBaseConfig configuration;
    private final Label label;

    private LinkPredictionComputation(
        Log log,
        GloballyScopedDependencies globallyScopedDependencies,
        CloseableResourceRegistry closeableResourceRegistry,
        DatabaseId databaseId,
        MemoryEstimationContext memoryEstimationContext,
        Metrics metrics,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        ProcedureReturnColumns procedureReturnColumns,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationMonitor terminationMonitor,
        User user,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        LinkPredictionPredictPipelineBaseConfig configuration,
        Label label,
        TrainedLPPipelineModel trainedLPPipelineModel
    ) {
        this.log = log;
        this.globallyScopedDependencies = globallyScopedDependencies;
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.databaseId = databaseId;
        this.memoryEstimationContext = memoryEstimationContext;
        this.metrics = metrics;
        this.nodePropertyExporterBuilder = nodePropertyExporterBuilder;
        this.procedureReturnColumns = procedureReturnColumns;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.requestCorrelationId = requestCorrelationId;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationMonitor = terminationMonitor;
        this.user = user;
        this.progressTrackerCreator = progressTrackerCreator;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.trainedLPPipelineModel = trainedLPPipelineModel;
        this.configuration = configuration;
        this.label = label;
    }

    static LinkPredictionComputation create(
        Log log,
        GloballyScopedDependencies globallyScopedDependencies,
        CloseableResourceRegistry closeableResourceRegistry,
        DatabaseId databaseId,
        MemoryEstimationContext memoryEstimationContext,
        Metrics metrics,
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        ProcedureReturnColumns procedureReturnColumns,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RequestCorrelationId requestCorrelationId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationMonitor terminationMonitor,
        User user,
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        LinkPredictionPredictPipelineBaseConfig configuration,
        Label label
    ) {
        var trainedNCPipelineModel = new TrainedLPPipelineModel(globallyScopedDependencies.modelCatalog());

        return new LinkPredictionComputation(
            log,
            globallyScopedDependencies,
            closeableResourceRegistry,
            databaseId,
            memoryEstimationContext,
            metrics,
            nodePropertyExporterBuilder,
            procedureReturnColumns,
            relationshipExporterBuilder,
            requestCorrelationId,
            taskRegistryFactory,
            terminationMonitor,
            user,
            progressTrackerCreator,
            algorithmsProcedureFacade,
            configuration,
            label,
            trainedNCPipelineModel
        );
    }

    @Override
    public LinkPredictionResult compute(Graph graph, GraphStore graphStore) {
        var model = trainedLPPipelineModel.get(
            configuration.modelName(),
            configuration.username()
        );
        var linkPredictionPipeline = model.customInfo().pipeline();

        var task = LinkPredictionPredictPipelineExecutor.progressTask(
            label.asString(),
            linkPredictionPipeline,
            graphStore,
            configuration
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(
            task,
            configuration.jobId(),
            configuration.concurrency(),
            configuration.logProgress()
        );

        // this is the literal worst. packing up request things, with application deps,
        // and shipping it blindly.
        var executionContext = new ExecutionContext(
            closeableResourceRegistry,
            databaseId,
            log,
            memoryEstimationContext,
            metrics,
            procedureReturnColumns,
            requestCorrelationId,
            taskRegistryFactory,
            terminationMonitor,
            user,
            algorithmsProcedureFacade,
            globallyScopedDependencies.modelCatalog(),
            nodePropertyExporterBuilder,
            relationshipExporterBuilder
        );

        var lpGraphStoreFilter = LPGraphStoreFilterFactory.generate(
            log,
            model.trainConfig(),
            configuration,
            graphStore
        );

        var pipelineExecutor = new LinkPredictionPredictPipelineExecutor(
            linkPredictionPipeline,
            ClassifierFactory.create(model.data()),
            lpGraphStoreFilter,
            configuration,
            executionContext,
            graphStore,
            progressTracker
        );

        return pipelineExecutor.compute();
    }
}
