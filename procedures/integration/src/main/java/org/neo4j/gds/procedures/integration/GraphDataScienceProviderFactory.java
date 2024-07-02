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

import org.neo4j.configuration.Config;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.DefaultMemoryGuard;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.modelcatalogservices.ModelCatalogServiceProvider;
import org.neo4j.gds.procedures.AlgorithmProcedureFacadeBuilderFactory;
import org.neo4j.gds.procedures.CatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserLogServices;

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
    private final Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final MemoryGauge memoryGauge;
    private final MetricsFacade metricsFacade;
    private final ModelCatalog modelCatalog;
    private final Config config;

    private GraphDataScienceProviderFactory(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        ExporterBuildersProviderService exporterBuildersProviderService,
        MemoryGauge memoryGauge,
        MetricsFacade metricsFacade,
        ModelCatalog modelCatalog,
        Config config
    ) {
        this.log = log;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.catalogBusinessFacadeDecorator = catalogBusinessFacadeDecorator;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.memoryGauge = memoryGauge;
        this.metricsFacade = metricsFacade;
        this.modelCatalog = modelCatalog;
        this.config = config;
    }

    GraphDataScienceProvider createGraphDataScienceProvider(
        TaskRegistryFactoryService taskRegistryFactoryService,
        boolean useMaxMemoryEstimation,
        UserLogServices userLogServices
    ) {
        var catalogProcedureFacadeFactory = new CatalogProcedureFacadeFactory(log);

        var algorithmFacadeBuilderFactory = createAlgorithmFacadeBuilderFactory(
            graphStoreCatalogService,
            useMaxMemoryEstimation
        );

        var memoryGuard = new DefaultMemoryGuard(log, useMaxMemoryEstimation, memoryGauge);

        return new GraphDataScienceProvider(
            log,
            defaultsConfiguration,
            limitsConfiguration,
            algorithmFacadeBuilderFactory,
            metricsFacade.algorithmMetrics(),
            algorithmProcessingTemplateDecorator,
            catalogBusinessFacadeDecorator,
            catalogProcedureFacadeFactory,
            metricsFacade.deprecatedProcedures(),
            exporterBuildersProviderService,
            graphStoreCatalogService,
            memoryGuard,
            metricsFacade.projectionMetrics(),
            taskRegistryFactoryService,
            userLogServices,
            this.config,
            modelCatalog
        );
    }

    static GraphDataScienceProviderFactory create(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogBusinessFacadeDecorator,
        ExporterBuildersProviderService exporterBuildersProviderService,
        MemoryGauge memoryGauge,
        MetricsFacade metricsFacade,
        ModelCatalog modelCatalog,
        Config config
    ) {
        return new GraphDataScienceProviderFactory(
            log,
            algorithmProcessingTemplateDecorator,
            catalogBusinessFacadeDecorator,
            exporterBuildersProviderService,
            memoryGauge,
            metricsFacade,
            modelCatalog,
            config
        );
    }

    private AlgorithmProcedureFacadeBuilderFactory createAlgorithmFacadeBuilderFactory(
        GraphStoreCatalogService graphStoreCatalogService,
        boolean useMaxMemoryEstimation
    ) {
        var modelCatalogServiceProvider = new ModelCatalogServiceProvider(modelCatalog);

        return new AlgorithmProcedureFacadeBuilderFactory(
            log,
            defaultsConfiguration,
            limitsConfiguration,
            graphStoreCatalogService,
            useMaxMemoryEstimation,
            metricsFacade.algorithmMetrics(),
            modelCatalogServiceProvider
        );
    }
}
