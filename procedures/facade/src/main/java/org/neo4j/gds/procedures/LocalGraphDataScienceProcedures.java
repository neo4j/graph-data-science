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
package org.neo4j.gds.procedures;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.DefaultAlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.graphstorecatalog.ExportLocation;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.procedures.algorithms.AlgorithmsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.catalog.GraphCatalogProcedureFacade;
import org.neo4j.gds.procedures.modelcatalog.LocalModelCatalogProcedureFacade;
import org.neo4j.gds.procedures.modelcatalog.ModelCatalogProcedureFacade;
import org.neo4j.gds.procedures.operations.LocalOperationsProcedureFacade;
import org.neo4j.gds.procedures.operations.OperationsProcedureFacade;
import org.neo4j.gds.procedures.pipelines.PipelineRepository;
import org.neo4j.gds.procedures.pipelines.PipelinesProcedureFacade;
import org.neo4j.gds.termination.TerminationMonitor;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Optional;
import java.util.function.Function;

public class LocalGraphDataScienceProcedures implements GraphDataScienceProcedures {
    private final Log log;

    private final AlgorithmsProcedureFacade algorithmsProcedureFacade;
    private final GraphCatalogProcedureFacade graphCatalogProcedureFacade;
    private final ModelCatalogProcedureFacade modelCatalogProcedureFacade;
    private final OperationsProcedureFacade operationsProcedureFacade;
    private final PipelinesProcedureFacade pipelinesProcedureFacade;

    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;

    /**
     * Keeping this package private to encourage use of @{@link GraphDataScienceProceduresBuilder}
     */
    LocalGraphDataScienceProcedures(
        Log log,
        AlgorithmsProcedureFacade algorithmsProcedureFacade,
        GraphCatalogProcedureFacade graphCatalogProcedureFacade,
        ModelCatalogProcedureFacade modelCatalogProcedureFacade,
        OperationsProcedureFacade operationsProcedureFacade,
        PipelinesProcedureFacade pipelinesProcedureFacade,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService
    ) {
        this.log = log;
        this.algorithmsProcedureFacade = algorithmsProcedureFacade;
        this.graphCatalogProcedureFacade = graphCatalogProcedureFacade;
        this.modelCatalogProcedureFacade = modelCatalogProcedureFacade;
        this.operationsProcedureFacade = operationsProcedureFacade;
        this.pipelinesProcedureFacade = pipelinesProcedureFacade;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
    }

    public static GraphDataScienceProcedures create(
        Log log,
        DefaultsConfiguration defaultsConfiguration,
        DependencyResolver dependencyResolver,
        ExportLocation exportLocation,
        GraphCatalogProcedureFacadeFactory graphCatalogProcedureFacadeFactory,
        FeatureTogglesRepository featureTogglesRepository,
        GraphStoreCatalogService graphStoreCatalogService,
        LimitsConfiguration limitsConfiguration,
        MemoryGuard memoryGuard,
        Metrics metrics,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        PipelineRepository pipelineRepository,
        GraphDatabaseService graphDatabaseService,
        KernelTransaction kernelTransaction,
        ProcedureReturnColumns procedureReturnColumns,
        RequestScopedDependencies requestScopedDependencies,
        TerminationMonitor terminationMonitor,
        Transaction procedureTransaction,
        WriteContext writeContext,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator
    ) {
        var closeableResourceRegistry = new TransactionCloseableResourceRegistry(kernelTransaction);

        var nodeLookup = new TransactionNodeLookup(kernelTransaction);

        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            requestScopedDependencies.getGraphLoaderContext(),
            requestScopedDependencies.getUser()
        );

        var algorithmEstimationTemplate = new AlgorithmEstimationTemplate(
            graphStoreCatalogService,
            databaseGraphStoreEstimationService,
            requestScopedDependencies
        );

        var algorithmProcessingTemplate = createAlgorithmProcessingTemplate(
            log,
            algorithmProcessingTemplateDecorator,
            graphStoreCatalogService,
            memoryGuard,
            metrics.algorithmMetrics(),
            requestScopedDependencies
        );

        var progressTrackerCreator = new ProgressTrackerCreator(log, requestScopedDependencies);

        var applicationsFacade = ApplicationsFacade.create(
            log,
            exportLocation,
            graphCatalogApplicationsDecorator,
            modelCatalogApplicationsDecorator,
            featureTogglesRepository,
            graphStoreCatalogService,
            metrics.projectionMetrics(),
            requestScopedDependencies,
            writeContext,
            modelCatalog,
            modelRepository,
            graphDatabaseService,
            procedureTransaction,
            progressTrackerCreator,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate
        );

        // merge these two
        var configurationParser = new ConfigurationParser(defaultsConfiguration, limitsConfiguration);
        var userSpecificConfigurationParser = new UserSpecificConfigurationParser(
            configurationParser,
            requestScopedDependencies.getUser()
        );

        var algorithmsProcedureFacade = AlgorithmsProcedureFacadeFactory.create(
            userSpecificConfigurationParser,
            requestScopedDependencies,
            kernelTransaction,
            applicationsFacade,
            procedureReturnColumns,
            algorithmEstimationTemplate
        );

        var graphCatalogProcedureFacade = graphCatalogProcedureFacadeFactory.createGraphCatalogProcedureFacade(
            applicationsFacade,
            graphDatabaseService,
            kernelTransaction,
            procedureTransaction,
            requestScopedDependencies,
            writeContext,
            procedureReturnColumns
        );

        var modelCatalogProcedureFacade = new LocalModelCatalogProcedureFacade(applicationsFacade);

        var operationsProcedureFacade = new LocalOperationsProcedureFacade(applicationsFacade);

        var pipelinesProcedureFacade = PipelinesProcedureFacade.create(
            log,
            modelCatalog,
            modelRepository,
            pipelineRepository,
            closeableResourceRegistry,
            requestScopedDependencies.getDatabaseId(),
            dependencyResolver,
            metrics,
            nodeLookup,
            writeContext.nodePropertyExporterBuilder(),
            procedureReturnColumns,
            writeContext.relationshipExporterBuilder(),
            requestScopedDependencies.getTaskRegistryFactory(),
            terminationMonitor,
            requestScopedDependencies.getTerminationFlag(),
            requestScopedDependencies.getUser(),
            requestScopedDependencies.getUserLogRegistryFactory(),
            progressTrackerCreator,
            algorithmsProcedureFacade,
            algorithmEstimationTemplate,
            algorithmProcessingTemplate
        );

        return new GraphDataScienceProceduresBuilder(log)
            .with(algorithmsProcedureFacade)
            .with(graphCatalogProcedureFacade)
            .with(modelCatalogProcedureFacade)
            .with(operationsProcedureFacade)
            .with(pipelinesProcedureFacade)
            .with(metrics.deprecatedProcedures())
            .build();
    }

    public Log log() {
        return log;
    }

    public AlgorithmsProcedureFacade algorithms() {
        return algorithmsProcedureFacade;
    }

    public GraphCatalogProcedureFacade graphCatalog() {
        return graphCatalogProcedureFacade;
    }

    public ModelCatalogProcedureFacade modelCatalog() {
        return modelCatalogProcedureFacade;
    }

    public OperationsProcedureFacade operations() {
        return operationsProcedureFacade;
    }

    public PipelinesProcedureFacade pipelines() {
        return pipelinesProcedureFacade;
    }

    public DeprecatedProceduresMetricService deprecatedProcedures() {
        return deprecatedProceduresMetricService;
    }

    private static AlgorithmProcessingTemplate createAlgorithmProcessingTemplate(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        GraphStoreCatalogService graphStoreCatalogService,
        MemoryGuard memoryGuard,
        AlgorithmMetricsService algorithmMetricsService,
        RequestScopedDependencies requestScopedDependencies
    ) {
        var algorithmProcessingTemplate = DefaultAlgorithmProcessingTemplate.create(
            log,
            algorithmMetricsService,
            graphStoreCatalogService,
            memoryGuard,
            requestScopedDependencies
        );

        if (algorithmProcessingTemplateDecorator.isEmpty()) return algorithmProcessingTemplate;

        return algorithmProcessingTemplateDecorator.get().apply(algorithmProcessingTemplate);
    }
}
