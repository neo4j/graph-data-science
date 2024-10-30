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
package org.neo4j.gds.projection;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.api.GraphStoreFactorySupplierProvider;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@ServiceProvider
public final class NativeFactorySupplierProvider implements GraphStoreFactorySupplierProvider {
    @Override
    public boolean canSupplyFactoryFor(GraphProjectConfig graphProjectConfig) {
        return graphProjectConfig instanceof GraphProjectFromStoreConfig;
    }

    @Override
    public GraphStoreFactory.Supplier supplier(GraphProjectConfig graphProjectConfig) {
        if (graphProjectConfig instanceof GraphProjectFromStoreConfig graphProjectFromStoreConfig) {
            return new GraphStoreFactory.Supplier() {
                @Override
                public GraphStoreFactory<? extends GraphStore, ? extends GraphProjectConfig> get(GraphLoaderContext loaderContext) {
                    return new NativeFactoryBuilder().graphProjectFromStoreConfig(graphProjectFromStoreConfig)
                        .loadingContext(loaderContext)
                        .build();
                }

                @Override
                public GraphStoreFactory<? extends GraphStore, ? extends GraphProjectConfig> getWithDimension(
                    GraphLoaderContext loaderContext, GraphDimensions graphDimensions
                ) {
                    return new NativeFactoryBuilder().graphProjectFromStoreConfig(graphProjectFromStoreConfig)
                        .loadingContext(loaderContext)
                        .graphDimensions(graphDimensions)
                        .build();
                }
            };
        }

        throw new IllegalArgumentException(formatWithLocale(
            "%s is not an instance of %s",
            graphProjectConfig.getClass().getName(),
            GraphProjectFromStoreConfig.class.getName()
        ));
    }
}
