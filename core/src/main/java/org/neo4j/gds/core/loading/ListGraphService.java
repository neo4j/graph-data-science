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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ListGraphService {
    public List<Pair<GraphStoreWithConfig, Map<String, Object>>> list(
        User user,
        Optional<String> graphName,
        boolean includeDegreeDistribution,
        TerminationFlag terminationFlag
    ) {
        var graphEntries = user.isAdmin()
            ? listAll()
            : listForUser(user);

        if (graphName.isPresent()) {
            // we should only list the provided graph
            graphEntries = graphEntries.filter(e -> e.getKey().graphName().equals(graphName.get()));
        }

        return graphEntries.map(e ->
        {
            GraphStoreWithConfig graphStoreWithConfig = GraphStoreWithConfig.of(e.getValue(), e.getKey());
            Map<String, Object> degreeDistribution = getOrCreateDegreeDistribution(
                includeDegreeDistribution,
                graphStoreWithConfig,
                terminationFlag
            );
            return Pair.of(graphStoreWithConfig, degreeDistribution);
        }).collect(Collectors.toList());
    }

    /**
     * This is get if cached; create if does not exist; or return null depending on the flag
     */
    private Map<String, Object> getOrCreateDegreeDistribution(
        boolean includeDegreeDistribution,
        GraphStoreWithConfig graphStoreWithConfig,
        TerminationFlag terminationFlag
    ) {
        if (!includeDegreeDistribution) return null;

        Optional<Map<String, Object>> maybeDegreeDistribution = GraphStoreCatalog.getDegreeDistribution(
            graphStoreWithConfig.config().username(),
            graphStoreWithConfig.graphStore().databaseId(),
            graphStoreWithConfig.config().graphName()
        );

        return maybeDegreeDistribution.orElseGet(() -> {
            var histogram = DegreeDistribution.compute(graphStoreWithConfig.graphStore().getUnion(), terminationFlag);
            // Cache the computed degree distribution in the Catalog
            GraphStoreCatalog.setDegreeDistribution(
                graphStoreWithConfig.config().username(),
                graphStoreWithConfig.graphStore().databaseId(),
                graphStoreWithConfig.config().graphName(),
                histogram
            );
            return histogram;
        });
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
