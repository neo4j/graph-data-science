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
import org.neo4j.gds.applications.graphstorecatalog.CatalogConfigurationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphNameValidationService;
import org.neo4j.gds.applications.graphstorecatalog.GraphStoreValidationService;
import org.neo4j.gds.beta.filter.GraphStoreFilterService;
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
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.ProcedureTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.procedures.TransactionContextAccessor;
import org.neo4j.gds.procedures.integration.AlgorithmMetaDataSetterService;
import org.neo4j.gds.procedures.integration.CatalogFacadeFactory;
import org.neo4j.gds.procedures.integration.CommunityProcedureFactory;
import org.neo4j.gds.procedures.integration.LogAdapter;
import org.neo4j.gds.procedures.integration.TaskRegistryFactoryProvider;
import org.neo4j.gds.procedures.integration.TaskStoreProvider;
import org.neo4j.gds.procedures.integration.UserLogRegistryFactoryProvider;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

import java.util.Optional;

/**
 * The OpenGDS extension for Neo4j.
 * We register a single component, @{@link org.neo4j.gds.procedures.GraphDataScience},
 * that all OpenGDS procedures can inject and use.
 */
@SuppressWarnings("unused")
@ServiceProvider
public class OpenGraphDataScienceExtension extends ExtensionFactory<OpenGraphDataScienceExtension.Dependencies> {
    // These are a few dull but widely used services
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final TerminationFlagService terminationFlagService = new TerminationFlagService();
    private final UserAccessor userAccessor = new UserAccessor();

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
        var neo4jConfig = dependencies.config();
        var progressTrackingEnabled = neo4jConfig.get(ProgressFeatureSettings.progress_tracking_enabled);
        log.info("Progress tracking: " + (progressTrackingEnabled ? "enabled" : "disabled"));
        var useMaxMemoryEstimation = neo4jConfig.get(MemoryEstimationSettings.validate_using_max_memory_estimation);
        log.info("Memory usage guard: " + (useMaxMemoryEstimation ? "maximum" : "minimum") + " estimate");

        // Task business is initialised from Neo4j configuration
        var taskStoreService = new TaskStoreService(progressTrackingEnabled);
        var taskRegistryFactoryService = new TaskRegistryFactoryService(progressTrackingEnabled, taskStoreService);

        // User log state will eventually be created here, instead of referencing a big shared singleton
        var userLogServices = new UserLogServices();

        // GDS services
        var graphStoreCatalogService = new GraphStoreCatalogService();

        // sub-interface factories
        var catalogFacadeFactory = createCatalogFacadeFactory(
            log,
            graphStoreCatalogService,
            databaseIdAccessor,
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            userLogServices,
            userAccessor
        );

        var communityProcedureFactory = createCommunityProcedureFactory(
            log,
            graphStoreCatalogService,
            useMaxMemoryEstimation,
            databaseIdAccessor,
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            userLogServices,
            userAccessor
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
        var taskStoreProvider = new TaskStoreProvider(databaseIdAccessor, taskStoreService);
        var taskRegistryFactoryProvider = new TaskRegistryFactoryProvider(
            databaseIdAccessor,
            userAccessor,
            taskRegistryFactoryService
        );
        var userLogRegistryFactoryProvider = new UserLogRegistryFactoryProvider(
            databaseIdAccessor,
            userAccessor,
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

    private static CatalogFacadeFactory createCatalogFacadeFactory(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseIdAccessor databaseIdService,
        KernelTransactionAccessor kernelTransactionAccessor,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserLogServices userLogServices,
        UserAccessor userServices
    ) {
        // there are some services that are currently only used for graph catalog, they can live here
        var catalogConfigurationService = new CatalogConfigurationService();
        var graphNameValidationService = new GraphNameValidationService();
        var graphStoreFilterService = new GraphStoreFilterService();
        var graphStoreValidationService = new GraphStoreValidationService();
        var procedureTransactionAccessor = new ProcedureTransactionAccessor();
        var transactionContextAccessor = new TransactionContextAccessor();

        return new CatalogFacadeFactory(
            catalogConfigurationService,
            log,
            graphNameValidationService,
            graphStoreCatalogService,
            graphStoreFilterService,
            graphStoreValidationService,
            procedureTransactionAccessor,
            databaseIdService,
            __ -> new NativeExportBuildersProvider(), // we always just offer native writes in OpenGDS
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            transactionContextAccessor,
            userLogServices,
            userServices,
            Optional.empty() // we have no extra checks to do in OpenGDS
        );
    }

    private static CommunityProcedureFactory createCommunityProcedureFactory(
        Log log,
        GraphStoreCatalogService graphStoreCatalogService,
        Boolean useMaxMemoryEstimation,
        DatabaseIdAccessor databaseIdService,
        KernelTransactionAccessor kernelTransactionAccessor,
        TaskRegistryFactoryService taskRegistryFactoryService,
        TerminationFlagService terminationFlagService,
        UserLogServices userLogServices,
        UserAccessor userServices
    ) {
        var algorithmMetaDataSetterService = new AlgorithmMetaDataSetterService();

        return new CommunityProcedureFactory(
            log,
            graphStoreCatalogService,
            useMaxMemoryEstimation,
            algorithmMetaDataSetterService,
            databaseIdService,
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            userLogServices,
            userServices
        );
    }

    public interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
