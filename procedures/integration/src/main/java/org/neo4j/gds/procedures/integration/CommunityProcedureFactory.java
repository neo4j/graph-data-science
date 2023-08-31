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
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsBusinessFacade;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsFacade;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserServices;
import org.neo4j.kernel.api.procedure.Context;

public class CommunityProcedureFactory {
    private final Log log;
    private final boolean useMaxMemoryEstimation;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final UserServices usernameService;
    private final DatabaseIdService databaseIdService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;

    public CommunityProcedureFactory(
        Log log,
        boolean useMaxMemoryEstimation,
        GraphStoreCatalogService graphStoreCatalogService,
        UserServices usernameService,
        DatabaseIdService databaseIdService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices
    ) {
        this.log = log;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.usernameService = usernameService;
        this.databaseIdService = databaseIdService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
    }

    CommunityProcedureFacade createCommunityProcedureFacade(Context context) {
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(
            log,
            useMaxMemoryEstimation
        );

        var databaseId = databaseIdService.getDatabaseId(context.graphDatabaseAPI());
        var user = usernameService.getUser(context.securityContext());
        var taskRegistryFactory = taskRegistryFactoryService.getTaskRegistryFactory(
            databaseId,
            user
        );

        var userLogRegistryFactory = userLogServices.getUserLogRegistryFactory(databaseId, user);

        // algorithm facade
        var communityAlgorithmsFacade = new CommunityAlgorithmsFacade(
            graphStoreCatalogService,
            taskRegistryFactory,
            userLogRegistryFactory,
            algorithmMemoryValidationService,
            log
        );

        // business facade
        var algorithmsBusinessFacade = new CommunityAlgorithmsBusinessFacade(communityAlgorithmsFacade, log);

        var returnColumns = new ProcedureCallContextReturnColumns(context.procedureCallContext());

        // procedure facade
        return new CommunityProcedureFacade(
            algorithmsBusinessFacade,
            returnColumns,
            databaseId,
            user
        );
    }
}
