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

import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.GraphStoreFactory;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;

public class NativeProjectionGraphStoreFactorySupplier implements GraphStoreFactory.Supplier {
    private final GraphProjectFromStoreConfig graphProjectFromStoreConfig;

    private NativeProjectionGraphStoreFactorySupplier(GraphProjectFromStoreConfig graphProjectFromStoreConfig) {
        this.graphProjectFromStoreConfig = graphProjectFromStoreConfig;
    }

    public static GraphStoreFactory.Supplier create(GraphProjectConfig graphProjectConfig) {
        if (graphProjectConfig instanceof GraphProjectFromStoreConfig graphProjectFromStoreConfig)
            return new NativeProjectionGraphStoreFactorySupplier(graphProjectFromStoreConfig);

        var problematicConfigurationType = graphProjectConfig.getClass().getSimpleName();

        throw new IllegalArgumentException("unable to create native supplier from " + problematicConfigurationType);
    }

    @Override
    public NativeFactory get(GraphLoaderContext loaderContext) {
        return new NativeFactoryBuilder()
            .graphProjectFromStoreConfig(graphProjectFromStoreConfig)
            .loadingContext(loaderContext)
            .build();
    }

    @Override
    public NativeFactory getWithDimension(GraphLoaderContext loaderContext, GraphDimensions graphDimensions) {
        return new NativeFactoryBuilder()
            .graphProjectFromStoreConfig(graphProjectFromStoreConfig)
            .loadingContext(loaderContext)
            .graphDimensions(graphDimensions)
            .build();
    }
}
