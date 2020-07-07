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

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class GraphStoreCatalog {

    private static final ConcurrentHashMap<String, UserCatalog> userCatalogs = new ConcurrentHashMap<>();

    private GraphStoreCatalog() { }

    public static GraphStoreWithConfig get(String username, NamedDatabaseId namedDatabaseId, String graphName) {
        return getUserCatalog(username).get(UserCatalog.UserCatalogKey.of(namedDatabaseId, graphName));
    }

    public static void set(GraphCreateConfig config, NamedDatabaseId namedDatabaseId, GraphStore graphStore) {
        graphStore.canRelease(false);
        userCatalogs.compute(config.username(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(UserCatalog.UserCatalogKey.of(namedDatabaseId, config.graphName()), config, graphStore);
            return userCatalog;
        });
    }

    public static Optional<Graph> getUnion(String username, NamedDatabaseId namedDatabaseId, String graphName) {
        return getUserCatalog(username).getUnion(UserCatalog.UserCatalogKey.of(namedDatabaseId, graphName));
    }

    public static boolean exists(String username, NamedDatabaseId namedDatabaseId, String graphName) {
        return getUserCatalog(username).exists(UserCatalog.UserCatalogKey.of(namedDatabaseId, graphName));
    }

    public static void remove(
        String username,
        NamedDatabaseId namedDatabaseId,
        String graphName,
        Consumer<GraphStoreWithConfig> removedGraphConsumer
    ) {
        UserCatalog.UserCatalogKey userCatalogKey = UserCatalog.UserCatalogKey.of(namedDatabaseId, graphName);

        GraphStoreWithConfig graphStoreWithConfig = Optional
            .ofNullable(getUserCatalog(username).remove(userCatalogKey))
            .orElseThrow(failOnNonExistentGraph(userCatalogKey.graphName()));

        removedGraphConsumer.accept(graphStoreWithConfig);
        GraphStore graphStore = graphStoreWithConfig.graphStore();
        graphStore.canRelease(true);
        graphStore.release();

        getUserCatalog(username).removeDegreeDistribution(userCatalogKey);
    }

    public static int graphStoresCount(NamedDatabaseId namedDatabaseId) {
        return (int) userCatalogs
            .values()
            .stream()
            .mapToLong(userCatalog -> userCatalog.getGraphStores(namedDatabaseId).values().size())
            .sum();
    }
    public static Optional<Map<String, Object>> getDegreeDistribution(String username, NamedDatabaseId namedDatabaseId, String graphName) {
        return getUserCatalog(username).getDegreeDistribution(UserCatalog.UserCatalogKey.of(namedDatabaseId, graphName));
    }

    public static void setDegreeDistribution(String username, NamedDatabaseId namedDatabaseId, String graphName, Map<String, Object> degreeDistribution) {
        getUserCatalog(username).setDegreeDistribution(UserCatalog.UserCatalogKey.of(namedDatabaseId, graphName), degreeDistribution);
    }

    public static void removeAllLoadedGraphs() {
        userCatalogs.clear();
    }

    public static Map<GraphCreateConfig, GraphStore> getGraphStores(String username, NamedDatabaseId namedDatabaseId) {
        return getUserCatalog(username).getGraphStores(namedDatabaseId);
    }

    private static UserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    private static Supplier<RuntimeException> failOnNonExistentGraph(String graphName) {
        return () -> new IllegalArgumentException(formatWithLocale(
            "Graph with name `%s` does not exist and can't be removed.",
            graphName
        ));
    }

    static class UserCatalog {

        @ValueClass
        public interface UserCatalogKey {

            String graphName();

            NamedDatabaseId namedDatabaseId();

            static UserCatalogKey of(GraphCreateConfig createConfig, NamedDatabaseId namedDatabaseId) {
                return of(namedDatabaseId, createConfig.graphName());
            }

            static UserCatalogKey of(NamedDatabaseId namedDatabaseId, String graphName) {
                return ImmutableUserCatalogKey.of(graphName, namedDatabaseId);
            }
        }

        private static final UserCatalog EMPTY = new UserCatalog();

        private final Map<UserCatalogKey, GraphStoreWithConfig> graphsByName = new ConcurrentHashMap<>();

        private final Map<UserCatalogKey, Map<String, Object>> degreeDistributionByName = new ConcurrentHashMap<>();

        private void set(UserCatalogKey userCatalogKey, GraphCreateConfig config, GraphStore graphStore) {
            if (config.graphName() == null || graphStore == null) {
                throw new IllegalArgumentException("Both name and graph store must be not null");
            }
            GraphStoreWithConfig graphStoreWithConfig = ImmutableGraphStoreWithConfig.of(graphStore, config);

            if (graphsByName.putIfAbsent(userCatalogKey, graphStoreWithConfig) != null) {
                throw new IllegalStateException(formatWithLocale(
                    "Graph name %s already loaded",
                    config.graphName()
                ));
            }
            graphStore.canRelease(false);
        }

        private void setDegreeDistribution(UserCatalogKey userCatalogKey, Map<String, Object> degreeDistribution) {

            if (userCatalogKey == null || degreeDistribution == null) {
                throw new IllegalArgumentException("Both name and degreeDistribution must be not null");
            }
            if (!graphsByName.containsKey(userCatalogKey)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Cannot set degreeDistribution because graph %s does not exist",
                    userCatalogKey.graphName()
                ));
            }
            degreeDistributionByName.put(userCatalogKey, degreeDistribution);
        }

        private void removeDegreeDistribution(UserCatalogKey userCatalogKey) {
            degreeDistributionByName.remove(userCatalogKey);
        }

        private GraphStoreWithConfig get(UserCatalogKey userCatalogKey) {
            if (graphsByName.containsKey(userCatalogKey)) {
                return graphsByName.get(userCatalogKey);
            } else {
                throw new NoSuchElementException(formatWithLocale("Cannot find graph with name '%s'.", userCatalogKey.graphName()));
            }
        }

        private Optional<Map<String, Object>> getDegreeDistribution(UserCatalogKey userCatalogKey) {
            if (!graphsByName.containsKey(userCatalogKey)) {
                return Optional.empty();
            }
            return Optional.ofNullable(degreeDistributionByName.get(userCatalogKey));
        }

        /**
         * A named graph is potentially split up into multiple sub-graphs.
         * Each sub-graph has the same node set and represents a unique relationship type / property combination.
         * This method returns the union of all subgraphs refered to by the given name.
         */
        private Optional<Graph> getUnion(UserCatalogKey userCatalogKey) {
            return !exists(userCatalogKey) ? Optional.empty() : Optional.of(graphsByName.get(userCatalogKey).graphStore().getUnion());
        }

        private boolean exists(UserCatalogKey userCatalogKey) {
            return userCatalogKey != null && graphsByName.containsKey(userCatalogKey);
        }

        @Nullable
        private GraphStoreWithConfig remove(UserCatalogKey userCatalogKey) {
            if (!exists(userCatalogKey)) {
                // remove is allowed to return null if the graph does not exist
                // as it's being used by algo.graph.info or algo.graph.remove,
                // that can deal with missing graphs
                return null;
            }
            return graphsByName.remove(userCatalogKey);
        }

        private Map<GraphCreateConfig, GraphStore> getGraphStores(NamedDatabaseId namedDatabaseId) {
            return graphsByName.entrySet().stream()
                .filter(entry -> entry.getKey().namedDatabaseId().equals(namedDatabaseId))
                .collect(Collectors.toMap(
                    entry -> entry.getValue().config(),
                    entry -> entry.getValue().graphStore()
                    )
                );
        }
    }

}
