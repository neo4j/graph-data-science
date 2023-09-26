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
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsEstimateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsMutateBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStatsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsStreamBusinessFacade;
import org.neo4j.gds.algorithms.community.NodePropertyService;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.procedures.KernelTransactionService;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserServices;
import org.neo4j.gds.transaction.DatabaseTransactionContext;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

public class CommunityProcedureFactory {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final AlgorithmMetaDataSetterService algorithmMetaDataSetterService;
    private final DatabaseIdService databaseIdService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final UserServices userServices;
    private final boolean useMaxMemoryEstimation;

    public CommunityProcedureFactory(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        AlgorithmMetaDataSetterService algorithmMetaDataSetterService,
        DatabaseIdService databaseIdService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        UserServices userServices,
        boolean useMaxMemoryEstimation
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.algorithmMetaDataSetterService = algorithmMetaDataSetterService;
        this.databaseIdService = databaseIdService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.userServices = userServices;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
    }

    public CommunityProcedureFacade createCommunityProcedureFacade(Context context) throws ProcedureException {
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(log, useMaxMemoryEstimation);
        var databaseId = databaseIdService.getDatabaseId(context.graphDatabaseAPI());
        var kernelTransactionService = new KernelTransactionService(context);
        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());
        var kernelTransaction = kernelTransactionService.getKernelTransaction();
        var user = userServices.getUser(context.securityContext());
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(
            databaseId,
            user
        );
        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);
        var algorithmMetaDataSetter = algorithmMetaDataSetterService.getAlgorithmMetaDataSetter(kernelTransaction);

        // algorithm facade
        var communityAlgorithmsFacade = new CommunityAlgorithmsFacade(
            graphStoreCatalogService,
            taskRegistryFactory,
            userLogRegistryFactory,
            algorithmMemoryValidationService,
            log
        );

        // moar services
        var fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();
        var graphLoaderContext = buildGraphLoaderContext(
            context,
            databaseId,
            taskRegistryFactory,
            userLogRegistryFactory
        );
        var databaseGraphStoreEstimationService = new DatabaseGraphStoreEstimationService(
            user,
            graphLoaderContext
        );

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
        var mutateBusinessFacade = new CommunityAlgorithmsMutateBusinessFacade(communityAlgorithmsFacade, new NodePropertyService(log));


        // procedure facade
        return new CommunityProcedureFacade(
            algorithmMetaDataSetter,
            databaseId,
            returnColumns,
            user,
            estimateBusinessFacade,
            mutateBusinessFacade,
            statsBusinessFacade,
            streamBusinessFacade
        );
    }

    private GraphLoaderContext buildGraphLoaderContext(
        Context context,
        DatabaseId databaseId,
        TaskRegistryFactory taskRegistryFactory,
        UserLogRegistryFactory userLogRegistryFactory
    ) throws ProcedureException {
        return ImmutableGraphLoaderContext
            .builder()
            .databaseId(databaseId)
            .dependencyResolver(context.dependencyResolver())
            .log((org.neo4j.logging.Log) log.getNeo4jLog())
            .taskRegistryFactory(taskRegistryFactory)
            .userLogRegistryFactory(userLogRegistryFactory)
            .terminationFlag(TerminationFlag.wrap(TerminationMonitor.EMPTY))
            .transactionContext(DatabaseTransactionContext.of(
                context.graphDatabaseAPI(),
                context.internalTransaction()
            ))
            .build();
    }
}
