/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class GraphCatalog extends GraphFactory {

    private static final ConcurrentHashMap<String, UserCatalog> userGraphCatalogs = new ConcurrentHashMap<>();

    public GraphCatalog(
        final GraphDatabaseAPI api,
        final GraphSetup setup
    ) {
        super(api, setup);
    }

    @Override
    protected Graph importGraph() {
        assert setup.relationshipPropertyMappings().numberOfMappings() <= 1;
        return get(
            setup.username(),
            setup.name(),
            setup.relationshipType(),
            setup.relationshipPropertyMappings().head().map(PropertyMapping::propertyKey)
        );
    }

    @Override
    public GraphsByRelationshipType importAllGraphs() {
        return GraphsByRelationshipType.of(importGraph());
    }

    @Override
    public Graph build() {
        return importGraph();
    }

    public MemoryEstimation memoryEstimation() {
        Graph graph = get(
            setup.username(),
            setup.name(),
            setup.relationshipType(),
            setup.relationshipPropertyMappings().head().map(PropertyMapping::propertyKey)
        );
        dimensions.nodeCount(graph.nodeCount());
        dimensions.maxRelCount(graph.relationshipCount());

        return HugeGraphFactory.getMemoryEstimation(setup, dimensions);
    }

    public static void set(GraphCreateConfig config, GraphsByRelationshipType graph) {
        graph.canRelease(false);
        userGraphCatalogs.compute(config.username(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(config, graph);
            return userCatalog;
        });
    }

    public static Graph get(
        String username,
        String graphName,
        String relationshipType,
        Optional<String> maybeRelationshipProperty
    ) {
        return getUserCatalog(username).get(graphName, relationshipType, maybeRelationshipProperty);
    }

    public static Optional<Graph> getUnion(String username, String graphName) {
        return getUserCatalog(username).getUnion(graphName);
    }

    public static boolean exists(String username, String graphName) {
        return getUserCatalog(username).exists(graphName);
    }

    public static @Nullable Graph remove(String username, String graphName) {
        return Optional
            .ofNullable(getUserCatalog(username).remove(graphName))
            .orElse(null);
    }

    public static void remove(String username, String graphName, Consumer<GraphWithConfig> graphRemovedConsumer) {
        GraphWithConfig graphWithConfig = Optional.ofNullable(getUserCatalog(username).removeWithoutRelease(graphName))
            .orElseThrow(failOnNonExistentGraph(graphName));

        graphRemovedConsumer.accept(graphWithConfig);

        Graph graph = graphWithConfig.getGraph();
        graph.canRelease(true);
        graph.release();
    }

    public static @Nullable String getType(String username, String graphName) {
        return getUserCatalog(username).getType(graphName);
    }

    public static Map<String, Graph> getLoadedGraphsByName(String username) {
        return getUserCatalog(username).getLoadedGraphsByName();
    }

    private static UserCatalog getUserCatalog(String username) {
        return userGraphCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    public static void removeAllLoadedGraphs() {
        userGraphCatalogs.clear();
    }

    public static Map<GraphCreateConfig, Graph> getLoadedGraphs(String username) {
        return getUserCatalog(username).getLoadedGraphs();
    }

    private static Supplier<RuntimeException> failOnNonExistentGraph(String graphName) {
        return () -> new IllegalArgumentException(String.format(
            "Graph with name `%s` does not exist and can't be removed.",
            graphName
        ));
    }

    public static Map<GraphCreateConfig, Graph> filterLoadedGraphs(
        String username,
        String graphName,
        List<String> relType,
        Optional<String> propertyName
    ) {
        return getUserCatalog(username).filterLoadedGraphs(graphName, relType, propertyName);
    }

    private static class UserCatalog {

        private static final UserCatalog EMPTY = new UserCatalog();

        private final Map<String, GraphWithConfig> graphsByName = new ConcurrentHashMap<>();

        void set(GraphCreateConfig config, GraphsByRelationshipType graph) {
            if (config.graphName() == null || graph == null) {
                throw new IllegalArgumentException("Both name and graph must be not null");
            }
            GraphWithConfig graphWithConfig = ImmutableGraphWithConfig.of(graph, config);
            if (graphsByName.putIfAbsent(config.graphName(), graphWithConfig) != null) {
                throw new IllegalStateException(String.format(
                    "Graph name %s already loaded",
                    config.graphName()
                ));
            }
            graph.canRelease(false);
        }

        @Deprecated
        Graph get(String graphName, String relationshipType, Optional<String> maybeRelationshipProperty) {
            if (!exists(graphName)) {
                throw new IllegalArgumentException(String.format("Graph with name '%s' does not exist.", graphName));
            }
            return graphsByName.get(graphName).graph().getGraph(relationshipType, maybeRelationshipProperty);
        }

        /**
         * A named graph is potentially split up into multiple sub-graphs.
         * Each sub-graph has the same node set and represents a unique relationship type / property combination.
         * This method returns the union of all subgraphs refered to by the given name.
         */
        Optional<Graph> getUnion(String graphName) {
            return !exists(graphName) ? Optional.empty() : Optional.of(graphsByName.get(graphName).graph().getUnion());
        }

        boolean exists(String graphName) {
            return graphName != null && graphsByName.containsKey(graphName);
        }

        @Nullable
        Graph remove(String graphName) {
            if (!exists(graphName)) {
                // remove is allowed to return null if the graph does not exist
                // as it's being used by algo.graph.info or algo.graph.remove,
                // that can deal with missing graphs
                return null;
            }
            GraphWithConfig graphWithConfig = graphsByName.remove(graphName);
            Graph graph = graphWithConfig.getGraph();
            graph.canRelease(true);
            graph.release();
            return graph;
        }

        @Nullable
        GraphWithConfig removeWithoutRelease(String graphName) {
            if (!exists(graphName)) {
                // remove is allowed to return null if the graph does not exist
                // as it's being used by algo.graph.info or algo.graph.remove,
                // that can deal with missing graphs
                return null;
            }
            return graphsByName.remove(graphName);
        }

        @Nullable
        String getType(String graphName) {
            if (graphName == null) return null;
            GraphsByRelationshipType graph = graphsByName.get(graphName).graph();
            return graph == null ? null : graph.getGraphType();
        }

        Map<String, Graph> getLoadedGraphsByName() {
            return graphsByName.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().getGraph()
            ));
        }

        Map<GraphCreateConfig, Graph> getLoadedGraphs() {
            return graphsByName.values().stream().collect(Collectors.toMap(
                GraphWithConfig::config, GraphWithConfig::getGraph
            ));
        }

        Map<GraphCreateConfig, Graph> filterLoadedGraphs(
            String graphName,
            List<String> relTypes,
            Optional<String> propertyName
        ) {
            Map<GraphCreateConfig, Graph> filteredGraphs = new HashMap<>();
            if (StringUtils.isBlank(graphName)) {
                graphsByName.values().forEach(gwc ->
                    filteredGraphs.put(gwc.config(), gwc.graph().getGraph(relTypes, propertyName))
                );
            } else {
                GraphWithConfig graphWithConfig = graphsByName.get(graphName);
                filteredGraphs.put(graphWithConfig.config(), graphWithConfig.graph().getGraph(relTypes, propertyName));
            }

            return filteredGraphs;
        }
    }

}
