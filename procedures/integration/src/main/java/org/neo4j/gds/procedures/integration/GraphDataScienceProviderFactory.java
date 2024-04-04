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

import org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryGauge;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.modelcatalogservices.ModelCatalogServiceProvider;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.services.UserLogServices;

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

    private final CatalogFacadeProviderFactory catalogFacadeProviderFactory;

    // Graph catalog state initialised here, currently just a front for a big shared singleton
    private final GraphStoreCatalogService graphStoreCatalogService = new GraphStoreCatalogService();

    private final Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator;
    private final ExporterBuildersProviderService exporterBuildersProviderService;
    private final MemoryGauge memoryGauge;
    private final MetricsFacade metricsFacade;
    private final ModelCatalog modelCatalog;

    private GraphDataScienceProviderFactory(
        Log log,
        CatalogFacadeProviderFactory catalogFacadeProviderFactory,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        ExporterBuildersProviderService exporterBuildersProviderService,
        MemoryGauge memoryGauge,
        MetricsFacade metricsFacade,
        ModelCatalog modelCatalog
    ) {
        this.log = log;
        this.catalogFacadeProviderFactory = catalogFacadeProviderFactory;
        this.algorithmProcessingTemplateDecorator = algorithmProcessingTemplateDecorator;
        this.exporterBuildersProviderService = exporterBuildersProviderService;
        this.memoryGauge = memoryGauge;
        this.metricsFacade = metricsFacade;
        this.modelCatalog = modelCatalog;
    }

    GraphDataScienceProvider createGraphDataScienceProvider(
        TaskRegistryFactoryService taskRegistryFactoryService,
        boolean useMaxMemoryEstimation,
        UserLogServices userLogServices
    ) {
        var catalogFacadeProvider = catalogFacadeProviderFactory.createCatalogFacadeProvider(
            graphStoreCatalogService,
            taskRegistryFactoryService,
            userLogServices
        );

        var algorithmFacadeService = createAlgorithmService(
            defaultsConfiguration,
            graphStoreCatalogService,
            limitsConfiguration,
            useMaxMemoryEstimation
        );

        return new GraphDataScienceProvider(
            log,
            catalogFacadeProvider,
            algorithmFacadeService,
            metricsFacade.deprecatedProcedures(),
            exporterBuildersProviderService,
            taskRegistryFactoryService,
            userLogServices,
            graphStoreCatalogService,
            algorithmProcessingTemplateDecorator,
            memoryGauge,
            useMaxMemoryEstimation,
            metricsFacade.algorithmMetrics()
        );
    }

    static GraphDataScienceProviderFactory create(
        Log log,
        Optional<Function<AlgorithmProcessingTemplate, AlgorithmProcessingTemplate>> algorithmProcessingTemplateDecorator,
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> catalogFacadeDecorator,
        ExporterBuildersProviderService exporterBuildersProviderService,
        MemoryGauge memoryGauge,
        MetricsFacade metricsFacade,
        ModelCatalog modelCatalog
    ) {
        var catalogFacadeProviderFactory = new CatalogFacadeProviderFactory(
            log,
            catalogFacadeDecorator,
            exporterBuildersProviderService,
            metricsFacade.projectionMetrics()
        );

        return new GraphDataScienceProviderFactory(
            log,
            catalogFacadeProviderFactory,
            algorithmProcessingTemplateDecorator,
            exporterBuildersProviderService,
            memoryGauge,
            metricsFacade,
            modelCatalog
        );
    }

    private AlgorithmFacadeFactoryProvider createAlgorithmService(
        DefaultsConfiguration defaultsConfiguration,
        GraphStoreCatalogService graphStoreCatalogService,
        LimitsConfiguration limitsConfiguration,
        boolean useMaxMemoryEstimation
    ) {
        // Defaults and limits is a big shared thing (or, will be)
        var configurationParser = new ConfigurationParser(defaultsConfiguration, limitsConfiguration);
        var modelCatalogServiceProvider = new ModelCatalogServiceProvider(modelCatalog);

        return new AlgorithmFacadeFactoryProvider(
            log,
            configurationParser,
            defaultsConfiguration,
            graphStoreCatalogService,
            limitsConfiguration,
            useMaxMemoryEstimation,
            metricsFacade.algorithmMetrics(),
            modelCatalogServiceProvider
        );
    }
}
