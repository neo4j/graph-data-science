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
package org.neo4j.gds.procedures.integration;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.DefaultAlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.pathfinding.DefaultMemoryGuard;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.procedures.DeprecatedProceduresMetricService;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.GraphDataScienceProceduresBuilder;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagAccessor;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

import java.util.Optional;
import java.util.function.Function;

/**
 * We use this at request time to construct the facade that the procedures call.
 */
public class GraphDataScienceProvider implements ThrowingFunction<Context, GraphDataScienceProcedures, ProcedureException> {
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final TerminationFlagAccessor terminationFlagAccessor = new TerminationFlagAccessor();
    private final UserAccessor userAccessor = new UserAccessor();

    private final Log log;
    private final CatalogFacadeProvider catalogFacadeProvider;
    private final AlgorithmFacadeFactoryProvider algorithmFacadeFactoryProvider;
    private final DeprecatedProceduresMetricService deprecatedProceduresMetricService;
    private final PipelinesProcedureFacadeProvider pipelinesProcedureFacadeProvider = new PipelinesProcedureFacadeProvider();
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final MemoryGauge memoryGauge;
    private final boolean useMaxMemoryEstimation;
    private final AlgorithmMetricsService algorithmMetricsService;

    GraphDataScienceProvider(
        Log log,
        CatalogFacadeProvider catalogFacadeProvider,
        AlgorithmFacadeFactoryProvider algorithmFacadeFactoryProvider,
        DeprecatedProceduresMetricService deprecatedProceduresMetricService,
        ExporterBuildersProviderService exporterBuildersProviderService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices, GraphStoreCatalogService graphStoreCatalogService,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        MemoryGauge memoryGauge, boolean useMaxMemoryEstimation, AlgorithmMetricsService algorithmMetricsService
    ) {
        this.log = log;
        this.catalogFacadeProvider = catalogFacadeProvider;
        this.algorithmFacadeFactoryProvider = algorithmFacadeFactoryProvider;
        this.deprecatedProceduresMetricService = deprecatedProceduresMetricService;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.memoryGauge = memoryGauge;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.algorithmMetricsService = algorithmMetricsService;
    }

    @Override
    public GraphDataScienceProcedures apply(Context context) throws ProcedureException {
        // with the context we can build the application layer before any of the procedure stuff
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        var databaseId = databaseIdAccessor.getDatabaseId(graphDatabaseService);
        var terminationFlag = terminationFlagAccessor.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());

        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var relationshipExporterBuilder = exportBuildersProvider.relationshipExporterBuilder(exporterContext);
        var relationshipStreamExporterBuilder = exportBuildersProvider.relationshipStreamExporterBuilder(exporterContext);

        var graphLoaderContext = GraphLoaderContextProvider.buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            log
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            graphLoaderContext,
            user
        );

        // this eventually goes below the fold, inside the application layer
        var algorithmEstimationTemplate = new AlgorithmEstimationTemplate(
            graphStoreCatalogService,
            databaseId,
            databaseGraphStoreEstimationService,
            user
        );

        var algorithmProcessingTemplate = buildAlgorithmProcessingTemplate(
            databaseId,
            user
        );

        var applicationsFacade = buildApplicationLayer(
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            algorithmProcessingTemplate,
            algorithmEstimationTemplate
        );

        var catalogProcedureFacade = catalogFacadeProvider.createCatalogProcedureFacade(context);

        var algorithmFacadeFactory = algorithmFacadeFactoryProvider.createAlgorithmFacadeFactory(
            context,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            taskRegistryFactory,
            userLogRegistryFactory,
            terminationFlag,
            databaseId,
            user,
            kernelTransaction,
            graphDatabaseService,
            databaseGraphStoreEstimationService
        );
        var centralityProcedureFacade = algorithmFacadeFactory.createCentralityProcedureFacade();
        var communityProcedureFacade = algorithmFacadeFactory.createCommunityProcedureFacade();
        var miscAlgorithmsProcedureFacade = algorithmFacadeFactory.createMiscellaneousProcedureFacade();
        var nodeEmbeddingsProcedureFacade = algorithmFacadeFactory.createNodeEmbeddingsProcedureFacade();
        var pathFindingProcedureFacade = algorithmFacadeFactory.createPathFindingProcedureFacade(applicationsFacade);
        var similarityProcedureFacade = algorithmFacadeFactory.createSimilarityProcedureFacade();

        var pipelinesProcedureFacade = pipelinesProcedureFacadeProvider.createPipelinesProcedureFacade(context);

        return new GraphDataScienceProceduresBuilder(log)
            .with(catalogProcedureFacade)
            .with(centralityProcedureFacade)
            .with(communityProcedureFacade)
            .with(miscAlgorithmsProcedureFacade)
            .with(nodeEmbeddingsProcedureFacade)
            .with(pathFindingProcedureFacade)
            .with(pipelinesProcedureFacade)
            .with(similarityProcedureFacade)
            .with(deprecatedProceduresMetricService)
            .build();
    }

    private AlgorithmProcessingTemplate buildAlgorithmProcessingTemplate(
        DatabaseId databaseId,
        User user
    ) {
        var memoryGuard = new DefaultMemoryGuard(log, useMaxMemoryEstimation, memoryGauge);

        var algorithmProcessingTemplate = new DefaultAlgorithmProcessingTemplate(
            log,
            algorithmMetricsService,
            graphStoreCatalogService,
            memoryGuard,
            databaseId,
            user
        );

        if (algorithmProcessingTemplateDecorator.isEmpty()) return algorithmProcessingTemplate;

        return algorithmProcessingTemplateDecorator.get().apply(algorithmProcessingTemplate);
    }

    private ApplicationsFacade buildApplicationLayer(
        NodePropertyExporterBuilder nodePropertyExporterBuilder,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        AlgorithmEstimationTemplate algorithmEstimationTemplate
    ) {
        return ApplicationsFacade.create(
            log,
            nodePropertyExporterBuilder,
            relationshipExporterBuilder,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory,
            algorithmProcessingTemplate,
            algorithmEstimationTemplate
        );
    }
}
