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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class GraphLoadFactory extends GraphFactory {

    private static final ConcurrentHashMap<String, GraphCatalog> userGraphCatalogs = new ConcurrentHashMap<>();
    private static final Map<String, Graph> EMPTY_MAP = new HashMap<>();

    public GraphLoadFactory(
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
        userGraphCatalogs.compute(username, ((user, graphCatalog) -> {
            if (graphCatalog == null) {
                graphCatalog = new GraphCatalog();
            }
            graphCatalog.set(graphName, graph);
            return graphCatalog;
        }));
    }

    public static Graph get(String username, String graphName, String relationshipType, Optional<String> maybeRelationshipProperty) {
        return getUserCatalog(username).get(graphName, relationshipType, maybeRelationshipProperty);
    }

    public static Graph getUnion(String username, String graphName) {
        return getUserCatalog(username).getUnion(graphName);
    }

    public static boolean exists(String username, String graphName) {
        return userCatalogExists(username) && userGraphCatalogs.get(username).exists(graphName);
    }

    public static Graph remove(String username, String graphName) {
        return userCatalogExists(username)
            ? getUserCatalog(username).remove(graphName)
            : null;
    }

    public static String getType(String username, String graphName) {
        return getUserCatalog(username).getType(graphName);
    }

    public static Map<String, Graph> getLoadedGraphs(String username) {
        return userCatalogExists(username)
            ? getUserCatalog(username).getLoadedGraphs()
            : EMPTY_MAP;
    }

    public static void removeAllLoadedGraphs() {
        userGraphCatalogs.clear();
    }

    private static boolean userCatalogExists(String username) {
        return username != null && userGraphCatalogs.containsKey(username);
    }

    private static GraphCatalog getUserCatalog(String username) {
        if (userCatalogExists(username)) {
            return userGraphCatalogs.get(username);
        } else {
            throw new IllegalArgumentException(String.format("No graphs stored for user '%s'.", username));
        }
    }
}
