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
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.applications.operations.FeatureTogglesRepository;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.procedures.integration.LogAccessor;
import org.neo4j.gds.procedures.integration.OpenGraphDataScienceExtensionBuilder;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.internal.LogService;

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
        var neo4jConfiguration = dependencies.config();

        var editionSpecifics = new OpenGraphDataScienceSpecificsBuilder(log, neo4jConfiguration).build();

        var databaseManagementService = dependencies.databaseManagementService();
        var dependencySatisfier = extensionContext.dependencySatisfier();
        var globalProcedures = dependencies.globalProcedures();

        // super annoying that some parts of our code defy dependency injection, so we need these as singletons
        var defaultsConfiguration = DefaultsConfiguration.Instance;
        var featureTogglesRepository = new FeatureTogglesRepository();
        var limitsConfiguration = LimitsConfiguration.Instance;

        var graphDataScienceExtensionBuilderAndAssociatedProducts = OpenGraphDataScienceExtensionBuilder.create(
            log,
            databaseManagementService,
            dependencySatisfier,
            globalProcedures,
            neo4jConfiguration,
            editionSpecifics,
            defaultsConfiguration,
            featureTogglesRepository,
            limitsConfiguration
        );

        var graphDataScienceExtensionBuilder = graphDataScienceExtensionBuilderAndAssociatedProducts.getLeft();

        return graphDataScienceExtensionBuilder.build();
    }

    public interface Dependencies {
        Config config();

        DatabaseManagementService databaseManagementService();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
