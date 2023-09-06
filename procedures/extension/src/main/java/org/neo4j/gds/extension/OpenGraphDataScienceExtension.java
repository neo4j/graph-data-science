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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.core.write.NativeExportBuildersProvider;
import org.neo4j.gds.internal.MemoryEstimationSettings;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.integration.CatalogFacadeFactory;
import org.neo4j.gds.procedures.integration.CommunityProcedureFactory;
import org.neo4j.gds.procedures.integration.LogAdapter;
import org.neo4j.gds.procedures.integration.TaskRegistryFactoryProvider;
import org.neo4j.gds.procedures.integration.TaskStoreProvider;
import org.neo4j.gds.procedures.integration.UserLogRegistryFactoryProvider;
import org.neo4j.gds.services.DatabaseIdService;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserServices;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

/**
 * The OpenGDS extension for Neo4j.
 * We register a single component, @{@link org.neo4j.gds.procedures.GraphDataScience},
 * that all OpenGDS procedures can inject and use.
 */
@SuppressWarnings("unused")
@ServiceProvider
public class OpenGraphDataScienceExtension extends ExtensionFactory<OpenGraphDataScienceExtension.Dependencies> {
    public OpenGraphDataScienceExtension() {
        super("gds.open");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        // We stack off the Neo4j's log and have our own
        var neo4jUserLog = Neo4jProxy.getUserLog(dependencies.logService(), getClass());
        Log gdsLog = new LogAdapter(neo4jUserLog);
        gdsLog.info("Building OpenGDS application...");

        registerComponents(dependencies, gdsLog);

        gdsLog.info("OpenGDS built and ready to go!");

        return new LifecycleAdapter();
    }

    private void registerComponents(Dependencies dependencies, Log log) {
        /*
         * Some things are needed both for the procedure facade, but also for legacy stuff, temporarily.
         * They are initialised here.
         */
        var databaseIdService = new DatabaseIdService();
        var neo4jConfig = dependencies.config();
        var progressTrackingEnabled = neo4jConfig.get(ProgressFeatureSettings.progress_tracking_enabled);
        log.info("Progress tracking: " + (progressTrackingEnabled ? "enabled" : "disabled"));
        var taskStoreService = new TaskStoreService(progressTrackingEnabled);
        var taskRegistryFactoryService = new TaskRegistryFactoryService(progressTrackingEnabled, taskStoreService);
        var useMaxMemoryEstimation = neo4jConfig.get(MemoryEstimationSettings.validate_using_max_memory_estimation);
        log.info("Memory usage guard: " + (useMaxMemoryEstimation ? "maximum" : "minimum") + " estimate");
        var userLogServices = new UserLogServices();
        var userServices = new UserServices();

        // GDS services
        var graphStoreCatalogService = new GraphStoreCatalogService();

        // sub-interface factories
        var catalogFacadeFactory = new CatalogFacadeFactory(
            log,
            graphStoreCatalogService,
            databaseIdService,
            __ -> new NativeExportBuildersProvider(), // we always just offer native writes in OpenGDS
            taskRegistryFactoryService,
            userLogServices,
            userServices
        );
        var communityProcedureFactory = new CommunityProcedureFactory(
            log,
            useMaxMemoryEstimation,
            graphStoreCatalogService,
            userServices,
            databaseIdService,
            taskRegistryFactoryService,
            userLogServices
        );

        // We need a provider to slot into the Neo4j Procedure Framework mechanism
        log.info("Register OpenGDS facade...");
        dependencies.globalProcedures().registerComponent(
            GraphDataScience.class,
            new OpenGraphDataScienceProcedureFacadeProvider(log, catalogFacadeFactory, communityProcedureFactory),
            true
        );
        log.info("OpenGDS facade registered.");

        /*
         * This is legacy support. We keep some context-injected things around,
         * but we look to strangle their usage soon.
         */
        var taskStoreProvider = new TaskStoreProvider(databaseIdService, taskStoreService);
        var taskRegistryFactoryProvider = new TaskRegistryFactoryProvider(
            databaseIdService,
            userServices,
            taskRegistryFactoryService
        );
        var userLogRegistryFactoryProvider = new UserLogRegistryFactoryProvider(
            databaseIdService,
            userServices,
            userLogServices
        );

        log.info("Register legacy Task Store/ Registry...");
        dependencies.globalProcedures().registerComponent(TaskStore.class, taskStoreProvider, true);
        dependencies.globalProcedures().registerComponent(TaskRegistryFactory.class, taskRegistryFactoryProvider, true);
        log.info("Task Registry registered.");

        log.info("Register legacy User Log Registry...");
        dependencies.globalProcedures().registerComponent(
            UserLogRegistryFactory.class,
            userLogRegistryFactoryProvider,
            true
        );
        log.info("User Log Registry registered.");
    }

    public interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
