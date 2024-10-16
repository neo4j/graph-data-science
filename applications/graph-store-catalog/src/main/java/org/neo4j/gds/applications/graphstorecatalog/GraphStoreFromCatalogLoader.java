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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.services.GraphDimensionFactory;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;

public final class GraphStoreFromCatalogLoader implements GraphStoreLoader {

    private final AlgoBaseConfig config;
    private final GraphStore graphStore;
    private final ResultStore resultStore;
    private final GraphProjectConfig graphProjectConfig;

    public GraphStoreFromCatalogLoader(
        String graphName,
        AlgoBaseConfig config,
        String username,
        DatabaseId databaseId,
        boolean isGdsAdmin
    ) {
        this.config = config;
        var catalogEntry = graphStoreFromCatalog(graphName, config, username, databaseId, isGdsAdmin);
        this.graphStore = catalogEntry.graphStore();
        this.resultStore = catalogEntry.resultStore();
        this.graphProjectConfig = catalogEntry.config();
    }

    @Override
    public GraphProjectConfig graphProjectConfig() {
        return this.graphProjectConfig;
    }

    @Override
    public GraphStore graphStore() {
        return this.graphStore;
    }

    @Override
    public ResultStore resultStore() {
        return this.resultStore;
    }

    @Override
    public GraphDimensions graphDimensions() {
        var graphStore = graphStore();
        var configuration = config;

        return new GraphDimensionFactory().create(graphStore, configuration);
    }

    private static GraphStoreCatalogEntry graphStoreFromCatalog(
        String graphName,
        BaseConfig config,
        String username,
        DatabaseId databaseId,
        boolean isGdsAdmin
    ) {
        var request = ImmutableCatalogRequest.of(
            databaseId.databaseName(),
            username,
            config.usernameOverride(),
            isGdsAdmin
        );
        return GraphStoreCatalog.get(request, graphName);
    }
}
