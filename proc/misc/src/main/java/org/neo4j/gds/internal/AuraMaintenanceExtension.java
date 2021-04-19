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
package org.neo4j.gds.internal;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public final class AuraMaintenanceFunctionExtension extends ExtensionFactory<AuraMaintenanceFunctionExtension.Dependencies> {

    @SuppressWarnings("unused - entry point for service loader")
    public AuraMaintenanceFunctionExtension() {
        super(ExtensionType.GLOBAL, "gds.aura.maintenance");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, AuraMaintenanceFunctionExtension.Dependencies dependencies) {
        var enabled = dependencies.config().get(AuraMaintenanceSettings.maintenance_function_enabled);
        if (enabled) {
            var registry = dependencies.globalProceduresRegistry();
            try {
                registry.register(new AuraMaintenanceFunction(), false);
            } catch (ProcedureException e) {
                dependencies.logService()
                    .getInternalLog(getClass())
                    .warn("Could not register aura maintenance function: " + e.getMessage(), e);
            }
        }
        return new LifecycleAdapter();
    }

    interface Dependencies {
        Config config();

        GlobalProcedures globalProceduresRegistry();

        LogService logService();
    }
}
