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

import org.neo4j.gds.applications.modelcatalog.ModelRepository;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.DefaultMemoryGuard;
import org.neo4j.gds.applications.graphstorecatalog.GraphCatalogApplications;
import org.neo4j.gds.applications.modelcatalog.ModelCatalogApplications;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.procedures.AlgorithmProcedureFacadeBuilderFactory;
import org.neo4j.gds.procedures.GraphCatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.graphdb.config.Configuration;

import java.util.Optional;
import java.util.function.Function;

/**
 * This is a way to squirrel away some dull code.
 * We want to keep Neo4j out from here, this could be reusable.
 */
final class GraphDataScienceProviderFactory {
    /**
     * These are currently global singletons; when they seize to be, this is the place to initialise them.
     * They are similar in lifecycle to {@link org.neo4j.gds.core.loading.GraphStoreCatalogService}
     */
    private final DefaultsConfiguration defaultsConfiguration = DefaultsConfiguration.Instance;
    private final LimitsConfiguration limitsConfiguration = LimitsConfiguration.Instance;

    private final Log log;

    // Graph catalog state initialised here, currently just a front for a big shared singleton
    private final GraphStoreCatalogService graphStoreCatalogService = new GraphStoreCatalogService();

    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator;
    private final Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final MemoryGauge memoryGauge;
    private final MetricsFacade metricsFacade;
    private final ModelCatalog modelCatalog;
    private final Configuration neo4jConfiguration;
    private final ModelRepository modelRepository;

    private GraphDataScienceProviderFactory(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator,
        ExporterBuildersProviderService exporterBuildersProviderService,
        MemoryGauge memoryGauge,
        MetricsFacade metricsFacade,
        ModelCatalog modelCatalog,
        Configuration neo4jConfiguration,
        ModelRepository modelRepository
    ) {
        this.log = log;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.graphCatalogApplicationsDecorator = graphCatalogApplicationsDecorator;
        this.modelCatalogApplicationsDecorator = modelCatalogApplicationsDecorator;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.memoryGauge = memoryGauge;
        this.metricsFacade = metricsFacade;
        this.modelCatalog = modelCatalog;
        this.neo4jConfiguration = neo4jConfiguration;
        this.modelRepository = modelRepository;
    }

    GraphDataScienceProvider createGraphDataScienceProvider(
        TaskRegistryFactoryService taskRegistryFactoryService,
        TaskStoreService taskStoreService,
        boolean useMaxMemoryEstimation,
        UserLogServices userLogServices
    ) {
        var catalogProcedureFacadeFactory = new GraphCatalogProcedureFacadeFactory(log);

        var algorithmFacadeBuilderFactory = createAlgorithmFacadeBuilderFactory(
            graphStoreCatalogService
        );

        var memoryGuard = new DefaultMemoryGuard(log, useMaxMemoryEstimation, memoryGauge);

        return new GraphDataScienceProvider(
            log,
            defaultsConfiguration,
            limitsConfiguration,
            algorithmFacadeBuilderFactory,
            metricsFacade.algorithmMetrics(),
            algorithmProcessingTemplateDecorator,
            graphCatalogApplicationsDecorator,
            modelCatalogApplicationsDecorator,
            catalogProcedureFacadeFactory,
            metricsFacade.deprecatedProcedures(),
            exporterBuildersProviderService,
            graphStoreCatalogService,
            memoryGuard,
            metricsFacade.projectionMetrics(),
            taskRegistryFactoryService,
            taskStoreService,
            userLogServices,
            neo4jConfiguration,
            modelCatalog,
            modelRepository
        );
    }

    static GraphDataScienceProviderFactory create(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<GraphCatalogApplications, GraphCatalogApplications>> graphCatalogApplicationsDecorator,
        Optional<Function<ModelCatalogApplications, ModelCatalogApplications>> modelCatalogApplicationsDecorator,
        ExporterBuildersProviderService exporterBuildersProviderService,
        MemoryGauge memoryGauge,
        MetricsFacade metricsFacade,
        ModelCatalog modelCatalog,
        Configuration neo4jConfiguration,
        ModelRepository modelRepository
    ) {
        return new GraphDataScienceProviderFactory(
            log,
            algorithmProcessingTemplateDecorator,
            graphCatalogApplicationsDecorator,
            modelCatalogApplicationsDecorator,
            exporterBuildersProviderService,
            memoryGauge,
            metricsFacade,
            modelCatalog,
            neo4jConfiguration,
            modelRepository
        );
    }

    private AlgorithmProcedureFacadeBuilderFactory createAlgorithmFacadeBuilderFactory(GraphStoreCatalogService graphStoreCatalogService) {
        return new AlgorithmProcedureFacadeBuilderFactory(
            defaultsConfiguration,
            limitsConfiguration,
            graphStoreCatalogService
        );
    }
}
