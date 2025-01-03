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
import org.neo4j.gds.core.write.NativeExportBuildersProvider;
import org.neo4j.gds.procedures.integration.ExporterBuildersComponentRegistration;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Life in OpenGDS is very simple, we just register native exporter builders as components.
 * They are database mode independent.
 * We need exporter builders as components since they are @Context injected into e.g. Pregel code.
 * Note that this is a database level extension.
 * It needs to be one because exporter builders can be database mode dependent.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
@ServiceProvider
public class OpenGdsExportBuildersExtension extends ExtensionFactory<OpenGdsExportBuildersExtension.Dependencies> {
    public OpenGdsExportBuildersExtension() {
        super(ExtensionType.DATABASE, "gds.open-write-services");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        var globalProcedures = dependencies.globalProcedures();
        var exporterBuildersComponentRegistration = new ExporterBuildersComponentRegistration(globalProcedures);

        var exportBuildersProvider = new NativeExportBuildersProvider();
        exporterBuildersComponentRegistration.registerExporterBuilders(exportBuildersProvider);

        return new LifecycleAdapter(); // do nothing
    }

    public interface Dependencies {
        GlobalProcedures globalProcedures();
    }
}
