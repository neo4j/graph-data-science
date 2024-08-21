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
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.core.model.OpenModelCatalogProvider;
import org.neo4j.gds.core.write.NativeExportBuildersProvider;
import org.neo4j.gds.metrics.MetricsFacade;
import org.neo4j.gds.procedures.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.integration.DefaultExportLocation;
import org.neo4j.gds.procedures.integration.LogAccessor;
import org.neo4j.gds.procedures.integration.OpenGraphDataScienceExtensionBuilder;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

import java.util.Optional;

/**
 * The OpenGDS extension for Neo4j.
 * We register a single component, @{@link org.neo4j.gds.procedures.GraphDataScienceProcedures},
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
        var log = new LogAccessor().getLog(dependencies.logService(), getClass());
        var globalProcedures = dependencies.globalProcedures();
        var neo4jConfiguration = dependencies.config();

        // OpenGDS edition customisations go here
        ExporterBuildersProviderService exporterBuildersProviderService = (__, ___) -> new NativeExportBuildersProvider(); // we always just offer native writes in OpenGDS
        var exportLocation = new DefaultExportLocation(log, neo4jConfiguration);
        var featureTogglesRepository = new FeatureTogglesRepository();
        var metricsFacade = MetricsFacade.PASSTHROUGH_METRICS_FACADE; // no metrics in OpenGDS
        var modelCatalog = new OpenModelCatalogProvider().get(null);
        var graphSageModelRepository = new DisableModelRepository(); // no model storing in OpenGDS

        var graphDataScienceExtensionBuilderAndAssociatedProducts = OpenGraphDataScienceExtensionBuilder.create(
            log,
            globalProcedures,
            neo4jConfiguration,
            exporterBuildersProviderService,
            exportLocation,
            featureTogglesRepository,
            metricsFacade,
            modelCatalog,
            graphSageModelRepository,
            Optional.empty(), // no extra checks in OpenGDS
            Optional.empty(), // no extra checks in OpenGDS
            Optional.empty() // no extra checks in OpenGDS
        );

        var graphDataScienceExtensionBuilder = graphDataScienceExtensionBuilderAndAssociatedProducts.getLeft();

        return graphDataScienceExtensionBuilder.build();
    }

    public interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
