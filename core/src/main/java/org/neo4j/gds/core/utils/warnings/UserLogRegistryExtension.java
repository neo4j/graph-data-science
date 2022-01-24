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
package org.neo4j.gds.core.utils.warnings;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.gds.core.utils.progress.ProgressFeatureSettings;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class UserLogRegistryExtension extends ExtensionFactory<UserLogRegistryExtension.Dependencies> {

    public UserLogRegistryExtension() {
        super(ExtensionType.DATABASE, "gds.warnings.registry");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, UserLogRegistryExtension.Dependencies dependencies) {
        var registry = dependencies.globalProceduresRegistry();
        var enabled = dependencies.config().get(ProgressFeatureSettings.progress_tracking_enabled);
        if (enabled) {
            var globalWarningStore = new GlobalUserLogStore();
            registry.registerComponent(UserLogStore.class, ctx -> globalWarningStore, true);
            registry.registerComponent(UserLogRegistryFactory.class, globalWarningStore, true);
        } else {
            registry.registerComponent(UserLogRegistryFactory.class, ctx -> EmptyUserLogRegistryFactory.INSTANCE, true);
            registry.registerComponent(UserLogStore.class, ctx -> EmptyUserLogStore.INSTANCE, true);

        }
        return new LifecycleAdapter();
    }

    interface Dependencies {
        Config config();

        LogService logService();

        GlobalProcedures globalProceduresRegistry();
    }
}
