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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is just an accessor that helps test other things more easily
 */
class GraphListingService {
    private final GraphStoreCatalogService graphStoreCatalogService;

    GraphListingService(GraphStoreCatalogService graphStoreCatalogService) {
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    List<Pair<GraphProjectConfig, GraphStore>> listGraphs(User user) {
        Stream<Pair<GraphProjectConfig, GraphStore>> pairStream = user.isAdmin()
            ? listAll()
            : listForUser(user);

        return pairStream.collect(Collectors.toList());
    }

    private Stream<Pair<GraphProjectConfig, GraphStore>> listAll() {
        return graphStoreCatalogService.getAllGraphStores()
            .map(graphStore -> Pair.of(
                    graphStore.config(),
                    graphStore.graphStore()
                )
            );
    }

    private Stream<Pair<GraphProjectConfig, GraphStore>> listForUser(User user) {
        return graphStoreCatalogService.getGraphStores(user).entrySet().stream()
            .map(entry -> Pair.of(
                entry.getKey(),
                entry.getValue()
            ));
    }
}
