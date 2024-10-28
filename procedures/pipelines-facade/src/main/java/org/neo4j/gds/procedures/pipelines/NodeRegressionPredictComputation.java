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
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.ml.models.Regressor;
import org.neo4j.gds.ml.models.linearregression.LinearRegressionData;
import org.neo4j.gds.ml.models.linearregression.LinearRegressor;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressor;
import org.neo4j.gds.ml.models.randomforest.RandomForestRegressorData;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.termination.TerminationMonitor;

final class NodeRegressionPredictComputation implements Computation<HugeDoubleArray> {
    private final Log log;
    private final ModelCatalog modelCatalog;

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

    private final ProgressTrackerCreator progressTrackerCreator;

    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;

    private final TrainedNRPipelineModel trainedNRPipelineModel;
    private final NodeRegressionPredictPipelineBaseConfig configuration;
    private final Label label;

    private NodeRegressionPredictComputation(
        Log log,
        ModelCatalog modelCatalog,
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
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        NodeRegressionPredictPipelineBaseConfig configuration,
        Label label,
        TrainedNRPipelineModel trainedNRPipelineModel
    ) {
        this.log = log;
        this.modelCatalog = modelCatalog;
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
        this.progressTrackerCreator = progressTrackerCreator;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.trainedNRPipelineModel = trainedNRPipelineModel;
        this.configuration = configuration;
        this.label = label;
    }

    static NodeRegressionPredictComputation create(
        Log log,
        ModelCatalog modelCatalog,
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
        ProgressTrackerCreator progressTrackerCreator,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        NodeRegressionPredictPipelineBaseConfig configuration,
        Label label
    ) {
        var trainedNRPipelineModel = new TrainedNRPipelineModel(modelCatalog);

        return new NodeRegressionPredictComputation(
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
            label,
            trainedNRPipelineModel
        );
    }

    @Override
    public HugeDoubleArray compute(Graph graph, GraphStore graphStore) {
        var model = trainedNRPipelineModel.get(
            configuration.modelName(),
            configuration.username()
        );

        var predictPipeline = model.customInfo().pipeline();

        var task = NodeRegressionPredictPipelineExecutor.progressTask(
            label.asString(),
            predictPipeline,
            graphStore
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

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

        return new NodeRegressionPredictPipelineExecutor(
            predictPipeline,
            configuration,
            executionContext,
            graphStore,
            progressTracker,
            regressorFrom(model.data())
        ).compute();
    }

    private Regressor regressorFrom(Regressor.RegressorData regressorData) {
        return switch (regressorData.trainerMethod()) {
            case LinearRegression -> new LinearRegressor((LinearRegressionData) regressorData);
            case RandomForestRegression -> new RandomForestRegressor((RandomForestRegressorData) regressorData);
            default -> throw new IllegalStateException("No such regressor: " + regressorData.trainerMethod().name());
        };
    }
}
