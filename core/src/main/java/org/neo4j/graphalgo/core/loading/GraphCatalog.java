/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class GraphCatalog extends GraphFactory {

    private static final ConcurrentHashMap<String, UserCatalog> userGraphCatalogs = new ConcurrentHashMap<>();

    public GraphCatalog(
            final GraphDatabaseAPI api,
            final GraphSetup setup) {
        super(api, setup);
    }

    @Override
    protected Graph importGraph() {
        assert setup.relationshipPropertyMappings.numberOfMappings() <= 1;
        return get(setup.username, setup.name, setup.relationshipType, setup.relationshipPropertyMappings.head().map(PropertyMapping::propertyKey));
    }

    @Override
    public Graph build() {
        return importGraph();
    }

    public MemoryEstimation memoryEstimation() {
        Graph graph = get(setup.username, setup.name, setup.relationshipType, setup.relationshipPropertyMappings.head().map(PropertyMapping::propertyKey));
        dimensions.nodeCount(graph.nodeCount());
        dimensions.maxRelCount(graph.relationshipCount());

        return HugeGraphFactory.getMemoryEstimation(setup, dimensions);
    }

    public static void set(String username, String graphName, GraphsByRelationshipType graph) {
        graph.canRelease(false);
        userGraphCatalogs.compute(username, (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(graphName, graph);
            return userCatalog;
        });
    }

    public static Graph get(String username, String graphName, String relationshipType, Optional<String> maybeRelationshipProperty) {
        return getUserCatalog(username).get(graphName, relationshipType, maybeRelationshipProperty);
    }

    public static Optional<Graph> getUnion(String username, String graphName) {
        return getUserCatalog(username).getUnion(graphName);
    }

    public static boolean exists(String username, String graphName) {
        return getUserCatalog(username).exists(graphName);
    }

    public static Graph remove(String username, String graphName) {
        return getUserCatalog(username).remove(graphName);
    }

    public static String getType(String username, String graphName) {
        return getUserCatalog(username).getType(graphName);
    }

    public static Map<String, Graph> getLoadedGraphs(String username) {
        return getUserCatalog(username).getLoadedGraphs();
    }

    private static UserCatalog getUserCatalog(String username) {
        return userGraphCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    public static void removeAllLoadedGraphs() {
        userGraphCatalogs.clear();
    }

    private static class UserCatalog {

        private static final UserCatalog EMPTY = new UserCatalog();

        private final Map<String, GraphsByRelationshipType> graphsByName = new ConcurrentHashMap<>();

        void set(String graphName, GraphsByRelationshipType graph) {
            if (graphName == null || graph == null) {
                throw new IllegalArgumentException("Both name and graph must be not null");
            }
            if (graphsByName.putIfAbsent(graphName, graph) != null) {
                throw new IllegalStateException("Graph name " + graphName + " already loaded");
            }
            graph.canRelease(false);
        }

        Graph get(String graphName, String relationshipType, Optional<String> maybeRelationshipProperty) {
            if (!exists(graphName)) {
                throw new IllegalArgumentException(String.format("Graph with name '%s' does not exist.", graphName));
            }
            return graphsByName.get(graphName).getGraph(relationshipType, maybeRelationshipProperty);
        }

        /**
         * A named graph is potentially split up into multiple sub-graphs.
         * Each sub-graph has the same node set and represents a unique relationship type / property combination.
         * This method returns the union of all subgraphs refered to by the given name.
         */
        Optional<Graph> getUnion(String graphName) {
            return !exists(graphName) ? Optional.empty() : Optional.of(graphsByName.get(graphName).getUnion());
        }

        boolean exists(String graphName) {
            return graphName != null && graphsByName.containsKey(graphName);
        }

        Graph remove(String graphName) {
            if (!exists(graphName)) {
                // remove is allowed to return null if the graph does not exist
                // as it's being used by algo.graph.info or algo.graph.remove,
                // that can deal with missing graphs
                return null;
            }
            Graph graph = graphsByName.remove(graphName).getUnion();
            graph.canRelease(true);
            graph.release();
            return graph;
        }

        String getType(String graphName) {
            if (graphName == null) return null;
            GraphsByRelationshipType graph = graphsByName.get(graphName);
            return graph == null ? null : graph.getGraphType();
        }

        Map<String, Graph> getLoadedGraphs() {
            return graphsByName.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().getUnion()
            ));
        }
    }
}
