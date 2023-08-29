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

import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsBusinessFacade;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserServices;
import org.neo4j.kernel.api.procedure.Context;

public class CommunityProcedureFactory {
    private final Log log;
    private final boolean useMaxMemoryEstimation;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final UserServices usernameService;
    private final DatabaseIdService databaseIdService;

    public CommunityProcedureFactory(
        Log log,
        boolean useMaxMemoryEstimation,
        GraphStoreCatalogService graphStoreCatalogService,
        UserServices usernameService,
        DatabaseIdService databaseIdService
    ) {
        this.log = log;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.usernameService = usernameService;
        this.databaseIdService = databaseIdService;
    }

    public CommunityProcedureFacade createCommunityProcedureFacade(Context context) {
        var algorithmMemoryValidationService = new AlgorithmMemoryValidationService(
            log,
            useMaxMemoryEstimation
        );

        // business facade
        var algorithmsBusinessFacade = new CommunityAlgorithmsBusinessFacade(
            graphStoreCatalogService,
            algorithmMemoryValidationService
        );

        // procedure facade
        return new CommunityProcedureFacade(
            algorithmsBusinessFacade,
            usernameService,
            databaseIdService,
            context.graphDatabaseAPI(),
            context.securityContext()
        );
    }
}
