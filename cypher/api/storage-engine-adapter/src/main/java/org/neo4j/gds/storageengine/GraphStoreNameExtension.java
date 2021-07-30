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
package org.neo4j.gds.storageengine;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.skip_default_indexes_on_creation;
import static org.neo4j.gds.storageengine.GraphStoreSettings.graph_name;

@ServiceProvider
public class GraphStoreNameExtension extends ExtensionFactory<GraphStoreNameExtension.Dependencies> {

    public GraphStoreNameExtension() {
        super(ExtensionType.DATABASE, "gds.graphstore.name");
    }

    @Override
    public Lifecycle newInstance(
        ExtensionContext context, Dependencies dependencies
    ) {
        return new LifecycleAdapter() {
            @Override
            public void init() {
                var config = dependencies.config();
                var graphName = InMemoryDatabaseCreationCatalog
                    .getRegisteredDbCreationGraphName(dependencies.databaseLayout().getDatabaseName());
                config.set(graph_name, graphName);
                config.set(skip_default_indexes_on_creation, true);
            }

            @Override
            public void start() {
                InMemoryDatabaseCreationCatalog.removeDbCreationRegistration(dependencies.databaseLayout().getDatabaseName());
            }

            @Override
            public void shutdown() {
                InMemoryDatabaseCreationCatalog.removeDbCreationRegistration(dependencies.databaseLayout().getDatabaseName());
            }

            @Override
            public void stop() {
                shutdown();
            }
        };
    }

    interface Dependencies {
        Config config();
        DatabaseLayout databaseLayout();
    }
}
