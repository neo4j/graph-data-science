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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.api.DatabaseId;

import java.util.concurrent.atomic.AtomicReference;

/**
 * One day the graph catalog won't be a static thing, it'll instead be a dependency you inject here. One day.
 * <p>
 * For now this service helps us engineer some other things.
 * Calls are mostly 1-1, but we can do some handy and _simple_ adapting, to make calling code easier to test,
 * without having to write separate tests for this class.
 */
public class GraphStoreCatalogService {
    public boolean graphExists(String username, DatabaseId databaseId, String graphName) {
        return GraphStoreCatalog.exists(username, databaseId, graphName);
    }

    public GraphStoreWithConfig removeGraph(
        CatalogRequest request,
        String graphName,
        boolean shouldFailIfMissing
    ) {
        var result = new AtomicReference<GraphStoreWithConfig>();
        GraphStoreCatalog.remove(
            request,
            graphName,
            result::set,
            shouldFailIfMissing
        );
        return result.get();
    }

    public GraphStoreWithConfig get(CatalogRequest catalogRequest, String graphName) {
        return GraphStoreCatalog.get(catalogRequest, graphName);
    }
}
