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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.internal.MemoryEstimationSettings;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.KernelTransactionAccessor;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.TerminationFlagService;
import org.neo4j.gds.services.DatabaseIdAccessor;
import org.neo4j.gds.services.UserAccessor;
import org.neo4j.gds.services.UserLogServices;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This is a general builder for extensions.
 * It includes convenience for the {@link org.neo4j.gds.procedures.GraphDataScience} component,
 * but allows arbitrary component registration.
 */
public final class ExtensionBuilder {
    // These are a few dull but widely used simple services
    private final DatabaseIdAccessor databaseIdAccessor = new DatabaseIdAccessor();
    private final KernelTransactionAccessor kernelTransactionAccessor = new KernelTransactionAccessor();
    private final TerminationFlagService terminationFlagService = new TerminationFlagService();
    private final UserAccessor userAccessor = new UserAccessor();

    // We support adding any number of components, in principle
    private final Collection<Runnable> registrations = new ArrayList<>();

    // Neo4j foundational services
    private final Log log;
    private final GlobalProcedures globalProcedures;

    // GDS state and services
    private final TaskStoreService taskStoreService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final UserLogServices userLogServices;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final boolean useMaxMemoryEstimation;

    private ExtensionBuilder(
        Log log,
        GlobalProcedures globalProcedures,
        TaskStoreService taskStoreService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        UserLogServices userLogServices,
        GraphStoreCatalogService graphStoreCatalogService,
        boolean useMaxMemoryEstimation
    ) {
        this.log = log;
        this.globalProcedures = globalProcedures;

        this.taskStoreService = taskStoreService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.userLogServices = userLogServices;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
    }

    /**
     * We create the extension builder purely from a few Neo4j services
     */
    public static ExtensionBuilder create(
        Log log,
        Configuration neo4jConfiguration,
        GlobalProcedures globalProcedures
    ) {
        // Read some configuration used to select behaviour
        var progressTrackingEnabled = neo4jConfiguration.get(ProgressFeatureSettings.progress_tracking_enabled);
        log.info("Progress tracking: " + (progressTrackingEnabled ? "enabled" : "disabled"));
        var useMaxMemoryEstimation = neo4jConfiguration.get(MemoryEstimationSettings.validate_using_max_memory_estimation);
        log.info("Memory usage guard: " + (useMaxMemoryEstimation ? "maximum" : "minimum") + " estimate");

        // Task business is initialised from Neo4j configuration
        var taskStoreService = new TaskStoreService(progressTrackingEnabled);
        var taskRegistryFactoryService = new TaskRegistryFactoryService(progressTrackingEnabled, taskStoreService);

        // User log state will eventually be created here, instead of referencing a big shared singleton
        var userLogServices = new UserLogServices();

        // Graph catalog state initialised here, currently just a front for a big shared singleton
        var graphStoreCatalogService = new GraphStoreCatalogService();

        return new ExtensionBuilder(
            log,
            globalProcedures,
            taskStoreService,
            taskRegistryFactoryService,
            userLogServices,
            graphStoreCatalogService,
            useMaxMemoryEstimation
        );
    }

    // Add a component to register
    public <T> ExtensionBuilder withComponent(
        Class<T> cls,
        Supplier<ThrowingFunction<Context, T, ProcedureException>> provider
    ) {
        registrations.add(() -> {
            log.info("Register " + cls.getSimpleName() + "...");
            globalProcedures.registerComponent(
                cls,
                provider.get(),
                true
            );
            log.info(cls.getSimpleName() + " registered.");
        });
        return this;
    }

    /**
     * The finalisation of the builder registers components with Neo4j
     */
    public void registerExtension() {
        // Process the list of components to register
        registrations.forEach(Runnable::run);

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
        globalProcedures.registerComponent(TaskStore.class, taskStoreProvider, true);
        globalProcedures.registerComponent(TaskRegistryFactory.class, taskRegistryFactoryProvider, true);
        log.info("Task Store/ Registry registered.");

        log.info("Register legacy User Log Registry...");
        globalProcedures.registerComponent(
            UserLogRegistryFactory.class,
            userLogRegistryFactoryProvider,
            true
        );
        log.info("User Log Registry registered.");
    }

    /**
     * Convenience for putting together the {@link org.neo4j.gds.procedures.GraphDataScience} provider.
     * You can customise a few things, but overall most things are dull here.
     *
     * @param exporterBuildersProviderService The catalog of writers
     * @param businessFacadeDecorator         Any checks added across requests
     */
    public ThrowingFunction<Context, GraphDataScience, ProcedureException> gdsProvider(
        ExporterBuildersProviderService exporterBuildersProviderService,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator
    ) {
        var catalogFacadeProvider = createCatalogFacadeProvider(
            exporterBuildersProviderService,
            businessFacadeDecorator
        );

        var communityProcedureProvider = createCommunityProcedureProvider();

        return new GraphDataScienceProvider(log, catalogFacadeProvider, communityProcedureProvider);
    }

    private CatalogFacadeProvider createCatalogFacadeProvider(
        ExporterBuildersProviderService exporterBuildersProviderService,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator
    ) {
        var catalogFacadeProviderFactory = new CatalogFacadeProviderFactory(
            log,
            exporterBuildersProviderService,
            businessFacadeDecorator
        );

        return catalogFacadeProviderFactory.createCatalogFacadeProvider(
            graphStoreCatalogService,
            databaseIdAccessor,
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            userLogServices,
            userAccessor
        );
    }

    private CommunityProcedureProvider createCommunityProcedureProvider() {
        var algorithmMetaDataSetterService = new AlgorithmMetaDataSetterService();

        return new CommunityProcedureProvider(
            log,
            graphStoreCatalogService,
            useMaxMemoryEstimation,
            algorithmMetaDataSetterService,
            databaseIdAccessor,
            kernelTransactionAccessor,
            taskRegistryFactoryService,
            terminationFlagService,
            userLogServices,
            userAccessor
        );
    }
}
