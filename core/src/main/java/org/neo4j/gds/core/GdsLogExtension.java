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
package org.neo4j.gds.core;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.logging.LogAdapter;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.internal.LogService;

@ServiceProvider
public class GdsLogExtension extends ExtensionFactory<GdsLogExtension.Dependencies> {

    public GdsLogExtension() {
        super(ExtensionType.GLOBAL, "gds.log");
    }

    @Override
    public Lifecycle newInstance(ExtensionContext context, Dependencies dependencies) {
        return LifecycleAdapter.onInit(() -> {
            var log = Neo4jProxy.getUserLog(dependencies.logService(), GdsLogExtension.class);
            GraphStoreCatalog.setLog(new LogAdapter(log));
        });
    }

    interface Dependencies {
        LogService logService();
    }
}
