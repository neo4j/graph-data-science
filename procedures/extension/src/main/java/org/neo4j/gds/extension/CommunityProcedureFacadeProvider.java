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
package org.neo4j.gds.extension;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.algorithms.community.CommunityAlgorithmsBusinessFacade;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

public class CommunityProcedureFacadeProvider implements ThrowingFunction<Context, CommunityProcedureFacade, ProcedureException> {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final UserServices usernameService;
    private final DatabaseIdService databaseIdService;
    private final boolean useMaxMemoryEstimation;

    CommunityProcedureFacadeProvider(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        UserServices usernameService,
        DatabaseIdService databaseIdService,
        boolean useMaxMemoryEstimation
    ) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.usernameService = usernameService;
        this.databaseIdService = databaseIdService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
    }

    @Override
    public CommunityProcedureFacade apply(Context context) throws ProcedureException {
        // services
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
