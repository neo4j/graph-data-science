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
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.StringSimilarity;
import org.neo4j.graphalgo.utils.StringJoining;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class GraphStoreCatalog {

    private static final ConcurrentHashMap<String, UserCatalog> userCatalogs = new ConcurrentHashMap<>();

    private GraphStoreCatalog() { }

    public static GraphStoreWithConfig get(String username, NamedDatabaseId databaseId, String graphName) {
        return getUserCatalog(username).get(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static void set(GraphCreateConfig config, GraphStore graphStore) {
        graphStore.canRelease(false);
        userCatalogs.compute(config.username(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(
                UserCatalog.UserCatalogKey.of(graphStore.databaseId(), config.graphName()),
                config,
                graphStore
            );
            return userCatalog;
        });
    }

    @TestOnly
    public static Optional<Graph> getUnion(String username, NamedDatabaseId databaseId, String graphName) {
        return getUserCatalog(username).getUnion(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static boolean exists(String username, NamedDatabaseId databaseId, String graphName) {
        return getUserCatalog(username).exists(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static void remove(
        String username,
        NamedDatabaseId databaseId,
        String graphName,
        Consumer<GraphStoreWithConfig> removedGraphConsumer,
        boolean failOnMissing
    ) {
        getUserCatalog(username).remove(
            UserCatalog.UserCatalogKey.of(databaseId, graphName),
            removedGraphConsumer,
            failOnMissing
        );
    }

    public static int graphStoresCount() {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.getGraphStores().values().size())
            .sum();
    }

    public static int graphStoresCount(NamedDatabaseId databaseId) {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.getGraphStores(databaseId).values().size())
            .sum();
    }

    public static Optional<Map<String, Object>> getDegreeDistribution(
        String username,
        NamedDatabaseId databaseId,
        String graphName
    ) {
        return getUserCatalog(username).getDegreeDistribution(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static void setDegreeDistribution(
        String username,
        NamedDatabaseId databaseId,
        String graphName,
        Map<String, Object> degreeDistribution
    ) {
        getUserCatalog(username).setDegreeDistribution(
            UserCatalog.UserCatalogKey.of(databaseId, graphName),
            degreeDistribution
        );
    }

    public static void removeAllLoadedGraphs() {
        userCatalogs.clear();
    }

    public static Map<GraphCreateConfig, GraphStore> getGraphStores(String username) {
        return getUserCatalog(username).getGraphStores();
    }

    public static Map<GraphCreateConfig, GraphStore> getGraphStores(String username, NamedDatabaseId databaseId) {
        return getUserCatalog(username).getGraphStores(databaseId);
    }

    private static UserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    static class UserCatalog {

        @ValueClass
        public interface UserCatalogKey {

            String graphName();

            NamedDatabaseId namedDatabaseId();

            static UserCatalogKey of(GraphCreateConfig createConfig, NamedDatabaseId databaseId) {
                return of(databaseId, createConfig.graphName());
            }

            static UserCatalogKey of(NamedDatabaseId databaseId, String graphName) {
                return ImmutableUserCatalogKey.of(graphName, databaseId);
            }
        }

        private static final UserCatalog EMPTY = new UserCatalog();

        private final Map<UserCatalogKey, GraphStoreWithConfig> graphsByName = new ConcurrentHashMap<>();

        private final Map<UserCatalogKey, Map<String, Object>> degreeDistributionByName = new ConcurrentHashMap<>();

        private void set(UserCatalogKey userCatalogKey, GraphCreateConfig config, GraphStore graphStore) {
            if (config.graphName() == null || graphStore == null) {
                throw new IllegalArgumentException("Both name and graph store must be not null");
            }
            GraphStoreWithConfig graphStoreWithConfig = GraphStoreWithConfig.of(graphStore, config);

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
            //noinspection ConstantConditions
            return get(userCatalogKey, true);
        }

        private @Nullable GraphStoreWithConfig get(UserCatalogKey userCatalogKey, boolean failOnMissing) {
            var graphStoreWithConfig = graphsByName.get(userCatalogKey);

            if (graphStoreWithConfig == null && failOnMissing) {
                var graphName = userCatalogKey.graphName();

                var availableGraphNames = graphsByName
                    .keySet()
                    .stream()
                    .map(UserCatalogKey::graphName)
                    .collect(Collectors.toList());

                var similarGraphNames = StringSimilarity.similarStrings(
                    graphName,
                    availableGraphNames
                );

                var exceptionMessage = similarGraphNames.isEmpty()
                    ? "."
                    : similarGraphNames.size() == 1
                        ? formatWithLocale(" (Did you mean `%s`?).", similarGraphNames.get(0))
                        : formatWithLocale(
                            " (Did you mean one of %s?).",
                            StringJoining.join(similarGraphNames, "`, `", "[`", "`]")
                        );

                throw new NoSuchElementException(formatWithLocale(
                    "Graph with name `%s` does not exist%s",
                    graphName,
                    exceptionMessage
                ));
            }

            return graphStoreWithConfig;
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
            return !exists(userCatalogKey) ? Optional.empty() : Optional.of(graphsByName
                .get(userCatalogKey)
                .graphStore()
                .getUnion());
        }

        private boolean exists(UserCatalogKey userCatalogKey) {
            return userCatalogKey != null && graphsByName.containsKey(userCatalogKey);
        }

        private void remove(
            UserCatalogKey userCatalogKey,
            Consumer<GraphStoreWithConfig> removedGraphConsumer,
            boolean failOnMissing
        ) {
            Optional.ofNullable(get(userCatalogKey, failOnMissing)).ifPresent(graphStoreWithConfig -> {
                removedGraphConsumer.accept(graphStoreWithConfig);
                graphStoreWithConfig.graphStore().canRelease(true);
                graphStoreWithConfig.graphStore().release();
                removeDegreeDistribution(userCatalogKey);
                graphsByName.remove(userCatalogKey);
            });
        }

        private Map<GraphCreateConfig, GraphStore> getGraphStores() {
            return graphsByName.values().stream()
                .collect(Collectors.toMap(
                    GraphStoreWithConfig::config,
                    GraphStoreWithConfig::graphStore
                    )
                );
        }

        private Map<GraphCreateConfig, GraphStore> getGraphStores(NamedDatabaseId databaseId) {
            return graphsByName.entrySet().stream()
                .filter(entry -> entry.getKey().namedDatabaseId().equals(databaseId))
                .collect(Collectors.toMap(
                    entry -> entry.getValue().config(),
                    entry -> entry.getValue().graphStore()
                    )
                );
        }
    }

}
