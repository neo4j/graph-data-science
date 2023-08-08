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
package org.neo4j.gds.procedure.facade;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.facade.AlgorithmsBusinessFacade;
import org.neo4j.gds.facade.CommunityProcedureFacade;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserServices;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;

public class CommunityProcedureFacadeProvider implements ThrowingFunction<Context, CommunityProcedureFacade, ProcedureException> {

    private final GraphStoreCatalogService graphStoreCatalogService;
    private final UserServices usernameService;
    private final DatabaseIdService databaseIdService;

    CommunityProcedureFacadeProvider(
        GraphStoreCatalogService graphStoreCatalogService,
        UserServices usernameService,
        DatabaseIdService databaseIdService
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.usernameService = usernameService;
        this.databaseIdService = databaseIdService;
    }

    @Override
    public CommunityProcedureFacade apply(Context context) throws ProcedureException {
        var algorithmsBusinessFacade = new AlgorithmsBusinessFacade(graphStoreCatalogService);
        return new CommunityProcedureFacade(
            algorithmsBusinessFacade,
            usernameService,
            databaseIdService,
            context.graphDatabaseAPI(),
            context.securityContext()
        );
    }
}
