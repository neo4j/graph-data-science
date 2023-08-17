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
import org.neo4j.gds.LicensingBusinessFacade;
import org.neo4j.gds.LicensingService;
import org.neo4j.gds.LicensingServiceBuilder;
import org.neo4j.gds.SysInfoProcFacade;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.gds.utils.PriorityServiceLoader.loadService;

@SuppressWarnings("unused")
@ServiceProvider
public class LicensingStateProcedureFacadeExtension extends ExtensionFactory<LicensingStateProcedureFacadeExtension.Dependencies> {
    public LicensingStateProcedureFacadeExtension() {
        super("gds.licensing.procedure_facade");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext extensionContext, Dependencies dependencies) {
        LicensingService licensingService = loadLicensingService(dependencies.config());

        dependencies.globalProcedures().registerComponent(
            SysInfoProcFacade.class,
            context -> new SysInfoProcFacade(new LicensingBusinessFacade(licensingService)),
            true
        );

        return new LifecycleAdapter();
    }

    private static LicensingService loadLicensingService(Config config) {
        var licensingServiceBuilder = loadService(
            LicensingServiceBuilder.class,
            LicensingServiceBuilder::priority
        );
        return licensingServiceBuilder.build(config);
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProcedures();
    }
}
