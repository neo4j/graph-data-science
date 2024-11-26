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

import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.graphstorecatalog.ExportLocation;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.mem.GcListenerExtension;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.TaskStore;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.memory.MemoryFacade;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.gds.settings.GdsSettings;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The GraphDataScience component has a certain contract,
 * in terms of the components it registers and the lifecycles it spawns.
 * We encapsulate it here, with allowances for customisations.
 */
public final class OpenGraphDataScienceExtensionBuilder {
    // fundamentals
    private final Log log;
    private final ComponentRegistration componentRegistration;

    // structural
    private final GraphDataScienceProceduresProviderFactory graphDataScienceProceduresProviderFactory;

    // edition specifics
    private final Metrics metrics;
    private final ModelCatalog modelCatalog;

    // services
    private final TaskStoreService taskStoreService;
    private final TaskRegistryFactoryService taskRegistryFactoryService;
    private final boolean useMaxMemoryEstimation;
    private final UserLogServices userLogServices;
    private final Lifecycle gcListener;

    private OpenGraphDataScienceExtensionBuilder(
        Log log,
        ComponentRegistration componentRegistration,
        GraphDataScienceProceduresProviderFactory graphDataScienceProceduresProviderFactory,
        Metrics metrics,
        ModelCatalog modelCatalog,
        TaskStoreService taskStoreService,
        TaskRegistryFactoryService taskRegistryFactoryService,
        boolean useMaxMemoryEstimation,
        UserLogServices userLogServices,
        Lifecycle gcListener
    ) {
        this.log = log;
        this.componentRegistration = componentRegistration;
        this.graphDataScienceProceduresProviderFactory = graphDataScienceProceduresProviderFactory;
        this.metrics = metrics;
        this.modelCatalog = modelCatalog;
        this.taskStoreService = taskStoreService;
        this.taskRegistryFactoryService = taskRegistryFactoryService;
        this.useMaxMemoryEstimation = useMaxMemoryEstimation;
        this.userLogServices = userLogServices;
        this.gcListener = gcListener;
    }

    /**
     * We want to build a GDS, we receive a few customisations and are able to read configuration,
     * and all the rest of the machinery goes here
     */
    public static Triple<OpenGraphDataScienceExtensionBuilder, TaskRegistryFactoryService, TaskStoreService> create(
        Log log,
        GlobalProcedures globalProcedures,
        Configuration neo4jConfiguration,
        ExporterBuildersProviderService exporterBuildersProviderService,
        ExportLocation exportLocation,
        FeatureTogglesRepository featureTogglesRepository,
        Metrics metrics,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator
    ) {
        // Read some configuration used to select behaviour
        var progressTrackingEnabled = neo4jConfiguration.get(ProgressFeatureSettings.progress_tracking_enabled);
        log.info("Progress tracking: " + (progressTrackingEnabled ? "enabled" : "disabled"));
        var useMaxMemoryEstimation = neo4jConfiguration.get(GdsSettings.validateUsingMaxMemoryEstimation());
        log.info("Memory usage guard: " + (useMaxMemoryEstimation ? "maximum" : "minimum") + " estimate");

        // Task business is initialised from Neo4j configuration
        var taskStoreService = new TaskStoreService(progressTrackingEnabled);
        var taskRegistryFactoryService = new TaskRegistryFactoryService(progressTrackingEnabled, taskStoreService);

        // User log state will eventually be created here, instead of referencing a big shared singleton
        var userLogServices = new UserLogServices();

        // Memory gauge integrates with the JVM
        // First, it is state held in an AtomicLong
        // Initialize with max available memory. Everything that is used at this point in time
        //  _could_ be garbage, and we want to err on the side of seeing more free heap.
        // It also has the effect that we allow all operations that theoretically fit into memory
        //  if the extension does never load.
        var availableMemory = Runtime.getRuntime().maxMemory();
        var freeMemoryAfterLastGc = new AtomicLong(availableMemory);
        // We make it available in a neat service
        var memoryTracker = new MemoryTracker(availableMemory, log);
        GraphStoreCatalog.registerGraphStoreAddedListener(memoryTracker);
        GraphStoreCatalog.registerGraphStoreRemovedListener(memoryTracker);

        // in the short term, until we eradicate old usages, we also install the shared state in its old place
        GcListenerExtension.setMemoryGauge(freeMemoryAfterLastGc);
        // State is populated from a GC listener
        var gcListener = new GcListenerInstaller(
            log,
            ManagementFactory.getGarbageCollectorMXBeans(),
            freeMemoryAfterLastGc
        );

        var componentRegistration = new ComponentRegistration(log, globalProcedures);

        var memoryFacade = new MemoryFacade(memoryTracker);

        componentRegistration.registerComponent("GDS Memory Facade", MemoryFacade.class, __ -> memoryFacade);

        var graphDataScienceProviderFactory = new GraphDataScienceProceduresProviderFactory(
            log,
            neo4jConfiguration,
            exporterBuildersProviderService,
            exportLocation,
            featureTogglesRepository,
            metrics,
            modelCatalog,
            modelRepository,
            algorithmProcessingTemplateDecorator,
            graphCatalogApplicationsDecorator,
            modelCatalogApplicationsDecorator,
            memoryTracker
        );

        var graphDataScienceExtensionBuilder = new OpenGraphDataScienceExtensionBuilder(
            log,
            componentRegistration,
            graphDataScienceProviderFactory,
            metrics,
            modelCatalog,
            taskStoreService,
            taskRegistryFactoryService,
            useMaxMemoryEstimation,
            userLogServices,
            gcListener
        );

        return Triple.of(graphDataScienceExtensionBuilder, taskRegistryFactoryService, taskStoreService);
    }

    /**
     * At this point we have all the bits ready, so we assemble and register them with Neo4j.
     * There are some legacy bits that are still part of the contract, they will disappear gradually,
     * and we will be left with just the one component.
     */
    public Lifecycle build() {
        log.info("Building Graph Data Science extension...");
        registerGraphDataScienceComponent();

        // register legacy bits
        registerMetricsComponent(metrics);
        registerModelCatalogComponent(modelCatalog);
        registerTaskRegistryFactoryComponent(taskRegistryFactoryService);
        registerTaskStoreComponent(taskStoreService);
        registerUserLogRegistryFactoryComponent(userLogServices);
        log.info("Graph Data Science extension built.");

        var lifeSupport = new LifeSupport();
        lifeSupport.add(gcListener);
        lifeSupport.add(new GraphStoreCatalogLogInitializer(log));
        return lifeSupport;
    }

    private void registerGraphDataScienceComponent() {
        var graphDataScienceProvider = graphDataScienceProceduresProviderFactory.createGraphDataScienceProvider(
            taskRegistryFactoryService,
            taskStoreService,
            useMaxMemoryEstimation,
            userLogServices
        );

        componentRegistration.registerComponent(
            "Graph Data Science",
            GraphDataScienceProcedures.class,
            graphDataScienceProvider
        );
    }

    /**
     * @deprecated Legacy stuff, will go away one day
     */
    @Deprecated
    private void registerMetricsComponent(Metrics metrics) {
        componentRegistration.registerComponent("Metrics", Metrics.class, __ -> metrics);
    }

    /**
     * @deprecated Legacy stuff, will go away one day
     */
    @Deprecated
    private void registerModelCatalogComponent(ModelCatalog modelCatalog) {
        componentRegistration.registerComponent("Model Catalog", ModelCatalog.class, __ -> modelCatalog);
    }

    /**
     * @param taskStoreService
     * @deprecated Legacy stuff, will go away one day
     */
    @Deprecated
    private void registerTaskStoreComponent(TaskStoreService taskStoreService) {
        var taskStoreProvider = new TaskStoreProvider(taskStoreService);

        componentRegistration.registerComponent("Task Store", TaskStore.class, taskStoreProvider);
    }

    /**
     * @deprecated Legacy stuff, will go away one day
     */
    @Deprecated
    private void registerTaskRegistryFactoryComponent(TaskRegistryFactoryService taskRegistryFactoryService) {
        var taskRegistryFactoryProvider = new TaskRegistryFactoryProvider(taskRegistryFactoryService);

        componentRegistration.registerComponent(
            "Task Registry Factory",
            TaskRegistryFactory.class,
            taskRegistryFactoryProvider
        );
    }

    /**
     * @deprecated Legacy stuff, will go away one day
     */
    @Deprecated
    private void registerUserLogRegistryFactoryComponent(UserLogServices userLogServices) {
        var userLogRegistryFactoryProvider = new UserLogRegistryFactoryProvider(userLogServices);

        componentRegistration.registerComponent(
            "User Log Registry Factory",
            UserLogRegistryFactory.class,
            userLogRegistryFactoryProvider
        );
    }
}
