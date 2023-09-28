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
import org.neo4j.gds.applications.graphstorecatalog.CatalogBusinessFacade;
import org.neo4j.gds.core.write.NativeExportBuildersProvider;
import org.neo4j.gds.procedures.GraphDataScience;
import org.neo4j.gds.procedures.integration.ExporterBuildersProviderService;
import org.neo4j.gds.procedures.integration.ExtensionBuilder;
import org.neo4j.gds.procedures.integration.LogAccessor;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

import java.util.Optional;
import java.util.function.Function;

/**
 * The OpenGDS extension for Neo4j.
 * We register a single component, @{@link org.neo4j.gds.procedures.GraphDataScience},
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

        var extensionBuilder = ExtensionBuilder.create(
            log, dependencies.config(),
            dependencies.globalProcedures()
        );

        // we always just offer native writes in OpenGDS
        ExporterBuildersProviderService exporterBuildersProviderService = __ -> new NativeExportBuildersProvider();
        // we have no extra checks to do in OpenGDS
        Optional<Function<CatalogBusinessFacade, CatalogBusinessFacade>> businessFacadeDecorator = Optional.empty();

        extensionBuilder
            .withComponent(
                GraphDataScience.class,
                () -> extensionBuilder.gdsProvider(exporterBuildersProviderService, businessFacadeDecorator)
            )
            .registerExtension();

        return new LifecycleAdapter();
    }

    public interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();

        LogService logService();
    }
}
