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
package org.neo4j.gds.procedures;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.gds.procedures.catalog.GraphStoreCatalogProcedureFacade;
import org.neo4j.gds.catalog.TaskRegistryFactoryService;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.procedures.community.CommunityProcedureFacade;
import org.neo4j.gds.internal.MemoryEstimationSettings;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserServices;
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
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        // We stack off the Neo4j's log and have our own
        // In terms of migration, this is good enough. We do not need state management,
        // and we can migrate in vertical chunks.
        var neo4jUserLog = Neo4jProxy.getUserLog(dependencies.logService(), getClass());
        Log gdsLog = new LogAdapter(neo4jUserLog);
        gdsLog.info("Building GDS application");

        setupProcedureFacade(dependencies, gdsLog);

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
        var usernameService = new UserServices();

        // GDS services
        var graphStoreCatalogService = new GraphStoreCatalogService();

        /*
         * Now we can register the facade
         */
        var procedureFacadeProvider = new ProcedureFacadeProvider(
            log,
            graphStoreCatalogService,
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
         * Now we can register the community algorithms procedure facade
         */
        boolean useMaxMemoryEstimation = neo4jConfig.get(MemoryEstimationSettings.validate_using_max_memory_estimation);
        log.info("Memory usage guard: " + (useMaxMemoryEstimation ? "maximum" : "minimum") + " estimate");

        var communityProcedureFacadeProvider = new CommunityProcedureFacadeProvider(
            log,
            graphStoreCatalogService,
            usernameService,
            databaseIdService,
            useMaxMemoryEstimation
        );

        log.info("Registering GDS Community Algorithms Procedure Facade");
        dependencies.globalProcedures().registerComponent(
            CommunityProcedureFacade.class,
            communityProcedureFacadeProvider,
            true
        );
        log.info("GDS Community Algorithms Procedure Facade registered");

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

    public interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
