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

import org.neo4j.gds.LicenseDetails;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.DefaultMemoryGuard;
import org.neo4j.gds.applications.graphstorecatalog.ExportLocation;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.logging.GdsLoggers;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.executor.MemoryEstimationContext;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.metrics.Metrics;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.GraphCatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserAccessor;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.gds.procedures.pipelines.PipelineRepository;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;
import org.neo4j.graphdb.config.Configuration;

import java.util.Optional;
import java.util.function.Function;

/**
 * This is a way to squirrel away some dull code.
 * We want to keep Neo4j out from here, this could be reusable.
 * PS: _Best_ class name ever, bar none.
 */
final class GraphDataScienceProceduresProviderFactory {
    // Pipeline repository state initialised here, currently just a front for a big shared singleton
    private final PipelineRepository pipelineRepository = new PipelineRepository();

    private final GdsLoggers loggers;

    private final Configuration neo4jConfiguration;
    private final DefaultsConfiguration defaultsConfiguration;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final ExportLocation exportLocation;
    private final FeatureTogglesRepository featureTogglesRepository;
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final GraphStoreFactorySuppliers graphStoreFactorySuppliers;
    private final LicenseDetails licenseDetails;
    private final LimitsConfiguration limitsConfiguration;
    private final Metrics metrics;
    private final ModelCatalog modelCatalog;
    private final ModelRepository modelRepository;
    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator;
    private final Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator;
    private final MemoryTracker memoryTracker;

    GraphDataScienceProceduresProviderFactory(
        GdsLoggers loggers,
        Configuration neo4jConfiguration,
        DefaultsConfiguration defaultsConfiguration,
        ExporterBuildersProviderService exporterBuildersProviderService,
        ExportLocation exportLocation,
        FeatureTogglesRepository featureTogglesRepository,
        GraphStoreCatalogService graphStoreCatalogService,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        LicenseDetails licenseDetails,
        LimitsConfiguration limitsConfiguration,
        Metrics metrics,
        ModelCatalog modelCatalog,
        ModelRepository modelRepository,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator,
        MemoryTracker memoryTracker
    ) {
        this.loggers = loggers;
        this.neo4jConfiguration = neo4jConfiguration;
        this.defaultsConfiguration = defaultsConfiguration;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.exportLocation = exportLocation;
        this.featureTogglesRepository = featureTogglesRepository;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.graphStoreFactorySuppliers = graphStoreFactorySuppliers;
        this.licenseDetails = licenseDetails;
        this.limitsConfiguration = limitsConfiguration;
        this.metrics = metrics;
        this.modelCatalog = modelCatalog;
        this.modelRepository = modelRepository;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.graphCatalogApplicationsDecorator = graphCatalogApplicationsDecorator;
        this.modelCatalogApplicationsDecorator = modelCatalogApplicationsDecorator;
        this.memoryTracker = memoryTracker;
    }

    GraphDataScienceProceduresProvider createGraphDataScienceProvider(
        TaskRegistryFactoryService taskRegistryFactoryService,
        TaskStoreService taskStoreService,
        boolean useMaxMemoryEstimation,
        UserLogServices userLogServices,
        UserAccessor userAccessor
    ) {
        var catalogProcedureFacadeFactory = new GraphCatalogProcedureFacadeFactory(
            loggers.log(),
            graphStoreFactorySuppliers
        );

        var memoryGuard = DefaultMemoryGuard.create(loggers.log(), useMaxMemoryEstimation, memoryTracker);

        return new GraphDataScienceProceduresProvider(
            loggers,
            neo4jConfiguration,
            defaultsConfiguration,
            exporterBuildersProviderService,
            exportLocation,
            catalogProcedureFacadeFactory,
            featureTogglesRepository,
            graphStoreCatalogService,
            graphStoreFactorySuppliers,
            licenseDetails,
            limitsConfiguration,
            memoryGuard,
            new MemoryEstimationContext(useMaxMemoryEstimation),
            metrics,
            modelCatalog,
            modelRepository,
            pipelineRepository,
            taskRegistryFactoryService,
            taskStoreService,
            userLogServices,
            algorithmProcessingTemplateDecorator,
            graphCatalogApplicationsDecorator,
            modelCatalogApplicationsDecorator,
            memoryTracker,
            userAccessor
        );
    }
}
