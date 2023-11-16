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
import org.neo4j.gds.algorithms.community.BasicAlgorithmRunner;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsWriteBusinessFacade;
import org.neo4j.gds.algorithms.community.MutateNodePropertyService;
import org.neo4j.gds.algorithms.community.WriteNodePropertyService;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.ExporterContext;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.procedures.community.ConfigurationParser;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

/**
 * We call it a provider because it is used as a sub-provider to the {@link org.neo4j.gds.procedures.GraphDataScience} provider.
 */
public class CommunityProcedureProvider {
    // Global state and services
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final boolean useMaxMemoryEstimation;

    // Request scoped state and services
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService;
    private final DatabaseIdAccessor databaseIdAccessor;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final KernelTransactionAccessor kernelTransactionAccessor;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final TerminationFlagService terminationFlagService;
    private final UserLogServices userLogServices;
    private final UserAccessor userAccessor;
    private final AlgorithmMetricsService algorithmMetricsService;

    public CommunityProcedureProvider(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        boolean useMaxMemoryEstimation,
        AlgorithmMetaDataSetterService algorithmMetaDataSetterService,
        DatabaseIdAccessor databaseIdAccessor,
        KernelTransactionAccessor kernelTransactionAccessor,
        ExporterBuildersProviderService exporterBuildersProviderService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserLogServices userLogServices,
        UserAccessor userAccessor,
        AlgorithmMetricsService algorithmMetricsService
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;

        this.algorithmMetaDataSetterService = algorithmMetaDataSetterService;
        this.databaseIdAccessor = databaseIdAccessor;
        this.kernelTransactionAccessor = kernelTransactionAccessor;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.terminationFlagService = terminationFlagService;
        this.userLogServices = userLogServices;
        this.userAccessor = userAccessor;
        this.algorithmMetricsService = algorithmMetricsService;
    }

    public CommunityProcedureFacade createCommunityProcedureFacade(Context context) throws ProcedureException {

        // Neo4j's services
        var graphDatabaseService = context.graphDatabaseAPI();

        var kernelTransaction = kernelTransactionAccessor.getKernelTransaction(context);

        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var databaseId = databaseIdAccessor.getDatabaseId(context.graphDatabaseAPI());
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var terminationFlag = terminationFlagService.createTerminationFlag(kernelTransaction);
        var user = userAccessor.getUser(context.securityContext());
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(
            databaseId,
            user
        );
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        var exportBuildersProvider = exporterBuildersProviderService.identifyExportBuildersProvider(graphDatabaseService);

        var algorithmRunner = new BasicAlgorithmRunner(
            graphStoreCatalogService,
            taskRegistryFactory,
            userLogRegistryFactory,
            algorithmMemoryValidationService,
            algorithmMetricsService,
            log
        );

        // algorithm facade
        var communityAlgorithmsFacade = new CommunityAlgorithmsFacade(algorithmRunner);

        // moar services
        var fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();
        var graphLoaderContext = buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            terminationFlag,
            userLogRegistryFactory
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            user,
            graphLoaderContext
        );

        var exporterContext = new ExporterContext.ProcedureContextWrapper(context);


        // business facades
        var estimateBusinessFacade = new CommunityAlgorithmsEstimateBusinessFacade(
            graphStoreCatalogService,
            fictitiousGraphStoreEstimationService,
            databaseGraphStoreEstimationService,
            databaseId,
            user
        );
        var statsBusinessFacade = new CommunityAlgorithmsStatsBusinessFacade(communityAlgorithmsFacade);
        var streamBusinessFacade = new CommunityAlgorithmsStreamBusinessFacade(communityAlgorithmsFacade);
        var mutateBusinessFacade = new CommunityAlgorithmsMutateBusinessFacade(
            communityAlgorithmsFacade,
            new MutateNodePropertyService(log)
        );
        CommunityAlgorithmsWriteBusinessFacade writeBusinessFacade = new CommunityAlgorithmsWriteBusinessFacade(
            communityAlgorithmsFacade,
            new WriteNodePropertyService(
                exportBuildersProvider.nodePropertyExporterBuilder(exporterContext),
                log,
                taskRegistryFactory
            )
        );
        var configurationParser = new ConfigurationParser(
            DefaultsConfiguration.Instance,
            LimitsConfiguration.Instance
        );
        // procedure facade
        return new CommunityProcedureFacade(
            configurationParser,
            algorithmMetaDataSetter,
            databaseId,
            returnColumns,
            user,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade,
            writeBusinessFacade
        );
    }

    private GraphLoaderContext buildGraphLoaderContext(
        Context context,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        UserLogRegistryFactory userLogRegistryFactory
    ) throws ProcedureException {
        return ImmutableGraphLoaderContext
            .builder()
            .databaseId(databaseId)
            .dependencyResolver(context.dependencyResolver())
            .log((org.neo4j.logging.Log) log.getNeo4jLog())
            .taskRegistryFactory(taskRegistryFactory)
            .userLogRegistryFactory(userLogRegistryFactory)
            .terminationFlag(terminationFlag)
            .transactionContext(DatabaseTransactionContext.of(
                context.graphDatabaseAPI(),
                context.internalTransaction()
            ))
            .build();
    }
}
