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
package org.neo4j.gds;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Optional;

final class GraphStoreFromCatalogLoader implements GraphStoreLoader {

    private final GraphStore graphStore;
    private final GraphCreateConfig graphCreateConfig;

    GraphStoreFromCatalogLoader(
        String graphName,
        BaseConfig config,
        String username,
        NamedDatabaseId databaseId,
        boolean isGdsAdmin
    ) {
        var graphStoreWithConfig = graphStoreFromCatalog(graphName, config, username, databaseId, isGdsAdmin);
        this.graphStore = graphStoreWithConfig.graphStore();
        this.graphCreateConfig = graphStoreWithConfig.config();
    }

    @Override
    public GraphCreateConfig graphCreateConfig() {
        return this.graphCreateConfig;
    }

    @Override
    public GraphStore graphStore() {
        return this.graphStore;
    }

    private GraphStoreWithConfig graphStoreFromCatalog(
        String graphName,
        BaseConfig config,
        String username,
        NamedDatabaseId databaseId,
        boolean isGdsAdmin
    ) {
        var request = ImmutableCatalogRequest.of(
            databaseId.name(),
            username,
            Optional.ofNullable(config.usernameOverride()),
            isGdsAdmin
        );
        return GraphStoreCatalog.get(request, graphName);
    }
}
