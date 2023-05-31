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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.catalog.DatabaseIdService;
import org.neo4j.gds.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.gds.catalog.UsernameService;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.loading.GraphNameValidationService;
import org.neo4j.gds.core.loading.GraphStoreCatalogBusinessFacade;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.PreconditionsService;
import org.neo4j.gds.executor.Preconditions;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

@SuppressWarnings("unused")
@ServiceProvider
public class GraphStoreCatalogProcedureFacadeExtension extends ExtensionFactory<GraphStoreCatalogProcedureFacadeExtension.Dependencies> {
    public GraphStoreCatalogProcedureFacadeExtension() {
        super("gds.procedure_facade");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        dependencies.globalProceduresRegistry().registerComponent(
            GraphStoreCatalogProcedureFacade.class,
            GraphStoreCatalogProcedureFacadeExtension::createFacade,
            true
        );
        Neo4jProxy.getInternalLog(dependencies.logService(), getClass()).info("Registered GDS procedure facade");
        return new LifecycleAdapter();
    }

    /**
     * The application is assembled here. Manage it well or complexity explodes.
     * This facade _is_ the application, the procedure stubs are just some dumb UI
     */
    private static GraphStoreCatalogProcedureFacade createFacade(Context context) {
        var preconditionsService = createPreconditionsService();
        var businessFacade = new GraphStoreCatalogBusinessFacade(
            preconditionsService,
            new GraphNameValidationService(),
            new GraphStoreCatalogService()
        );

        var usernameService = new UsernameService(context.securityContext());
        var databaseIdService = new DatabaseIdService(context.graphDatabaseAPI());

        return new GraphStoreCatalogProcedureFacade(
            usernameService,
            databaseIdService,
            businessFacade
        );
    }

    private static PreconditionsService createPreconditionsService() {
        return Preconditions::check;
    }

    interface Dependencies {
        GlobalProcedures globalProceduresRegistry();

        LogService logService();
    }
}
