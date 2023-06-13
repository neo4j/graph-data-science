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
import org.neo4j.configuration.Config;
import org.neo4j.gds.catalog.DatabaseIdService;
import org.neo4j.gds.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.gds.catalog.TaskRegistryFactoryService;
import org.neo4j.gds.catalog.UserLogServices;
import org.neo4j.gds.catalog.UsernameService;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

@SuppressWarnings("unused")
@ServiceProvider
public class GraphStoreCatalogProcedureFacadeExtension extends ExtensionFactory<GraphStoreCatalogProcedureFacadeExtension.Dependencies> {
    public GraphStoreCatalogProcedureFacadeExtension() {
        super("gds.procedure_facade");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        // we shall replace this Neo4j Log with a GDS Log down the line
        var log = Neo4jProxy.getUserLog(dependencies.logService(), getClass());
        log.info("Building GDS application");

        setupProcedureFacade(dependencies, log);

        return new LifecycleAdapter();
    }

    private void setupProcedureFacade(Dependencies dependencies, Log log) {
        /*
         * Some things are needed both for the procedure facade, but also for legacy stuff, temporarily.
         * They go here, so that ProcedureFacadeProvider is solely responsible for ProcedureFacade.
         */
        var databaseIdService = new DatabaseIdService();
        var neo4jConfig = dependencies.config();
        var progressTrackingEnabled = neo4jConfig.get(ProgressFeatureSettings.progress_tracking_enabled);
        log.info("Progress tracking: " + (progressTrackingEnabled ? "enabled" : "disabled"));
        var taskStoreService = new TaskStoreService(progressTrackingEnabled);
        var taskRegistryFactoryService = new TaskRegistryFactoryService(progressTrackingEnabled, taskStoreService);
        var userLogServices = new UserLogServices();
        var usernameService = new UsernameService();

        /*
         * Now we can register the facade
         */
        var procedureFacadeProvider = new ProcedureFacadeProvider(
            log,
            databaseIdService,
            taskRegistryFactoryService,
            userLogServices,
            usernameService
        );

        log.info("Registering GDS Procedure Facade");
        dependencies.globalProcedures().registerComponent(
            GraphStoreCatalogProcedureFacade.class,
            procedureFacadeProvider,
            true
        );
        log.info("GDS procedure facade registered");

        /*
         * This is legacy support. We keep some context-injected things around,
         * but we look to strangle their usage soon.
         */
        var taskStoreProvider = new TaskStoreProvider(databaseIdService, taskStoreService);
        var taskRegistryFactoryProvider = new TaskRegistryFactoryProvider(
            databaseIdService,
            usernameService,
            taskRegistryFactoryService
        );
        var userLogRegistryFactoryProvider = new UserLogRegistryFactoryProvider(
            databaseIdService,
            usernameService,
            userLogServices
        );

        log.info("Register legacy Task Store/ Registry");
        dependencies.globalProcedures().registerComponent(TaskStore.class, taskStoreProvider, true);
        dependencies.globalProcedures().registerComponent(TaskRegistryFactory.class, taskRegistryFactoryProvider, true);
        log.info("Task Registry registered");

        log.info("Register legacy User Log Registry");
        dependencies.globalProcedures().registerComponent(
            UserLogRegistryFactory.class,
            userLogRegistryFactoryProvider,
            true
        );
        log.info("User Log Registry registered");
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
