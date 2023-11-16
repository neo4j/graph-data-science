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

import org.neo4j.gds.ProcedureCallContextReturnColumns;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.estimation.AlgorithmEstimator;
import org.neo4j.gds.algorithms.metrics.AlgorithmMetricsService;
import org.neo4j.gds.algorithms.mutateservices.MutateNodePropertyService;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.algorithms.writeservices.WriteNodePropertyService;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.algorithms.ConfigurationCreator;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.configparser.ConfigurationParser;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * We call it a provider because it is used as a sub-provider to the {@link org.neo4j.gds.procedures.GraphDataScience} provider.
 */
public class CommunityProcedureProvider {
    // dull static stuff
    private final FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();

    // Global state and services
    private final Log log;
    private final ConfigurationParser configurationParser;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final boolean useMaxMemoryEstimation;

    // Request scoped state and services
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final DatabaseIdAccessor databaseIdAccessor;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final KernelTransactionAccessor kernelTransactionAccessor;
    private final MutateNodePropertyService mutateNodePropertyService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TerminationFlagService terminationFlagService;
    private final UserAccessor userAccessor;
    private final UserLogServices userLogServices;

    public CommunityProcedureProvider(
        Log log,
        ConfigurationParser configurationParser,
        GraphStoreCatalogService graphStoreCatalogService,
        boolean useMaxMemoryEstimation,
        AlgorithmMetaDataSetterService algorithmMetaDataSetterService,
        AlgorithmMetricsService algorithmMetricsService,
        DatabaseIdAccessor databaseIdAccessor,
        ExporterBuildersProviderService exporterBuildersProviderService,
        KernelTransactionAccessor kernelTransactionAccessor,
        MutateNodePropertyService mutateNodePropertyService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserAccessor userAccessor, UserLogServices userLogServices
    ) {
        // fundamental
        this.log = log;

        // global state
        this.configurationParser = configurationParser;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;

        // request scope
        this.algorithmMetaDataSetterService = algorithmMetaDataSetterService;
        this.algorithmMetricsService = algorithmMetricsService;
        this.databaseIdAccessor = databaseIdAccessor;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.kernelTransactionAccessor = kernelTransactionAccessor;
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.terminationFlagService = terminationFlagService;
        this.userAccessor = userAccessor;
        this.userLogServices = userLogServices;
    }

    public CommunityProcedureFacade createCommunityProcedureFacade(Context context) throws ProcedureException {
        // Neo4j's services
        var graphDatabaseService = context.graphDatabaseAPI();
        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        // GDS derived services - request scoped things
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var databaseId = databaseIdAccessor.getDatabaseId(context.graphDatabaseAPI());
        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);
        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);
        var nodePropertyExporterBuilder = exportBuildersProvider.nodePropertyExporterBuilder(exporterContext);
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var terminationFlag = terminationFlagService.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());
        var configurationCreator = new ConfigurationCreator(configurationParser, algorithmMetaDataSetter, user);
        var requestScopedDependencies = RequestScopedDependencies.builder()
            .with(databaseId)
            .with(terminationFlag)
            .with(user)
            .build();
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(databaseId, user);
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);
        var writeNodePropertyService = new WriteNodePropertyService(
            log,
            nodePropertyExporterBuilder,
            taskRegistryFactory,
            terminationFlag
        );
        var algorithmRunner = new AlgorithmRunner(
            log,
            graphStoreCatalogService,
            algorithmMetricsService,
            algorithmMemoryValidationService,
            requestScopedDependencies,
            taskRegistryFactory,
            userLogRegistryFactory
        );
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
        var algorithmEstimator = new AlgorithmEstimator(
            graphStoreCatalogService,
            fictitiousGraphStoreEstimationService,
            databaseGraphStoreEstimationService,
            databaseId,
            user
        );

        // algorithm facade
        var communityAlgorithmsFacade = new CommunityAlgorithmsFacade(algorithmRunner);

        // mode-specific facades
        var estimateBusinessFacade = new CommunityAlgorithmsEstimateBusinessFacade(algorithmEstimator);
        var mutateBusinessFacade = new CommunityAlgorithmsMutateBusinessFacade(
            mutateNodePropertyService,
            communityAlgorithmsFacade
        );
        var statsBusinessFacade = new CommunityAlgorithmsStatsBusinessFacade(communityAlgorithmsFacade);
        var streamBusinessFacade = new CommunityAlgorithmsStreamBusinessFacade(communityAlgorithmsFacade);
        var writeBusinessFacade = new CommunityAlgorithmsWriteBusinessFacade(
            communityAlgorithmsFacade,
            writeNodePropertyService
        );

        // procedure facade
        return new CommunityProcedureFacade(
            configurationCreator,
            returnColumns,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }
}
