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
package org.neo4j.gds.legacycypherprojection;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.projection.GraphStoreFactorySupplier;

public final class CypherProjectionGraphStoreFactorySupplier implements GraphStoreFactorySupplier {
    private final Log log;
    private final GraphProjectFromCypherConfig graphProjectFromCypherConfig;

    private CypherProjectionGraphStoreFactorySupplier(
        Log log,
        GraphProjectFromCypherConfig graphProjectFromCypherConfig
    ) {
        this.log = log;
        this.graphProjectFromCypherConfig = graphProjectFromCypherConfig;
    }

    @Override
    public CypherFactory get(
        GraphLoaderContext loaderContext,
        DependencyResolver dependencyResolver,
        RequestCorrelationId requestCorrelationId
    ) {
        return CypherFactory.createWithDerivedDimensions(
            graphProjectFromCypherConfig,
            loaderContext,
            dependencyResolver,
            log,
            requestCorrelationId
        );
    }

    @Override
    public CypherFactory getWithDimension(
        GraphLoaderContext loaderContext,
        GraphDimensions graphDimensions,
        DependencyResolver dependencyResolver,
        RequestCorrelationId requestCorrelationId
    ) {
        return CypherFactory.createWithBaseDimensions(
            graphProjectFromCypherConfig,
            loaderContext,
            graphDimensions,
            dependencyResolver,
            log,
            requestCorrelationId
        );
    }

    public static CypherProjectionGraphStoreFactorySupplier create(Log log, GraphProjectConfig graphProjectConfig) {
        if (graphProjectConfig instanceof GraphProjectFromCypherConfig graphProjectFromCypherConfig)
            return new CypherProjectionGraphStoreFactorySupplier(log, graphProjectFromCypherConfig);

        var problematicConfigurationType = graphProjectConfig.getClass().getSimpleName();

        throw new IllegalArgumentException("unable to create cypher supplier from " + problematicConfigurationType);
    }
}
