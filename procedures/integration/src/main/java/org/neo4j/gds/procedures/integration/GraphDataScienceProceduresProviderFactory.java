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
import org.neo4j.gds.applications.algorithms.machinery.DefaultMemoryGuard;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.utils.logging.GdsLoggers;
import org.neo4j.gds.core.utils.progress.TaskStoreService;
import org.neo4j.gds.domain.services.GloballyScopedDependencies;
import org.neo4j.gds.executor.MemoryEstimationContext;
import org.neo4j.gds.mem.MemoryTracker;
import org.neo4j.gds.procedures.GraphCatalogProcedureFacadeFactory;
import org.neo4j.gds.procedures.TaskRegistryFactoryService;
import org.neo4j.gds.procedures.UserAccessor;
import org.neo4j.gds.procedures.UserLogServices;
import org.neo4j.gds.procedures.pipelines.PipelineRepository;
import org.neo4j.gds.projection.GraphStoreFactorySuppliers;
import org.neo4j.graphdb.config.Configuration;

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
    private final OpenGraphDataScienceSpecifics openGraphDataScienceSpecifics;
    private final GloballyScopedDependencies globallyScopedDependencies;
    private final DefaultsConfiguration defaultsConfiguration;
    private final FeatureTogglesRepository featureTogglesRepository;
    private final GraphStoreFactorySuppliers graphStoreFactorySuppliers;
    private final LicenseDetails licenseDetails;
    private final LimitsConfiguration limitsConfiguration;
    private final MemoryTracker memoryTracker;

    GraphDataScienceProceduresProviderFactory(
        GdsLoggers loggers,
        Configuration neo4jConfiguration,
        OpenGraphDataScienceSpecifics openGraphDataScienceSpecifics,
        GloballyScopedDependencies globallyScopedDependencies,
        DefaultsConfiguration defaultsConfiguration,
        FeatureTogglesRepository featureTogglesRepository,
        GraphStoreFactorySuppliers graphStoreFactorySuppliers,
        LicenseDetails licenseDetails,
        LimitsConfiguration limitsConfiguration,
        MemoryTracker memoryTracker
    ) {
        this.loggers = loggers;
        this.neo4jConfiguration = neo4jConfiguration;
        this.openGraphDataScienceSpecifics = openGraphDataScienceSpecifics;
        this.globallyScopedDependencies = globallyScopedDependencies;
        this.defaultsConfiguration = defaultsConfiguration;
        this.featureTogglesRepository = featureTogglesRepository;
        this.graphStoreFactorySuppliers = graphStoreFactorySuppliers;
        this.licenseDetails = licenseDetails;
        this.limitsConfiguration = limitsConfiguration;
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
            openGraphDataScienceSpecifics,
            globallyScopedDependencies,
            defaultsConfiguration,
            catalogProcedureFacadeFactory,
            featureTogglesRepository,
            graphStoreFactorySuppliers,
            licenseDetails,
            limitsConfiguration,
            memoryGuard,
            new MemoryEstimationContext(useMaxMemoryEstimation),
            pipelineRepository,
            taskRegistryFactoryService,
            taskStoreService,
            userLogServices,
            memoryTracker,
            userAccessor
        );
    }
}
