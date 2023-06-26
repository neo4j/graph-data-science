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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Port of GraphListOperator
 */
public class ListGraphService {
    public List<GraphStoreWithConfig> list(User user, Optional<String> graphName) {
        var graphEntries = user.isAdmin()
            ? listAll()
            : listForUser(user);

        if (graphName.isPresent()) {
            // we should only list the provided graph
            graphEntries = graphEntries.filter(e -> e.getKey().graphName().equals(graphName.get()));
        }

        return graphEntries.map(e -> GraphStoreWithConfig.of(e.getValue(), e.getKey())).collect(Collectors.toList());
    }

    private static Stream<Map.Entry<GraphProjectConfig, GraphStore>> listAll() {
        return GraphStoreCatalog.getAllGraphStores()
            .map(graphStore -> Map.entry(
                    graphStore.config(),
                    graphStore.graphStore()
                )
            );
    }

    private static Stream<Map.Entry<GraphProjectConfig, GraphStore>> listForUser(User user) {
        return GraphStoreCatalog.getGraphStores(user.getUsername()).entrySet().stream();
    }
}
