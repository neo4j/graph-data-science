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

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.utils.StringJoining;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GraphStoreCatalog {

    private static final ConcurrentHashMap<String, UserCatalog> userCatalogs = new ConcurrentHashMap<>();

    private GraphStoreCatalog() { }

    public static GraphStoreWithConfig get(CatalogRequest request, String graphName) {
        var userCatalogKey = UserCatalog.UserCatalogKey.of(request.databaseName(), graphName);
        var ownCatalog = getUserCatalog(request.username());

        var maybeGraph = ownCatalog.get(userCatalogKey, request.restrictSearchToUsernameCatalog());
        if (maybeGraph != null) {
            return maybeGraph;
        }

        var usersWithMatchingGraphs = userCatalogs
            .entrySet()
            .stream()
            .flatMap(e -> Stream
                .ofNullable(e.getValue().get(userCatalogKey, false))
                .map(graph -> Map.entry(e.getKey(), graph)))
            .collect(Collectors.toList());

        if (usersWithMatchingGraphs.size() == 1) {
            return usersWithMatchingGraphs.get(0).getValue();
        }

        if (usersWithMatchingGraphs.isEmpty()) {
            // suggests only own graphs names
            throw ownCatalog.graphNotFoundException(userCatalogKey);
        }

        var usernames = StringJoining.joinVerbose(
            usersWithMatchingGraphs.stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet())
        );

        throw new IllegalArgumentException(formatWithLocale(
            "Multiple graphs that match '%s' are found from the users %s.",
            graphName,
            usernames
        ));
    }

    public static void remove(
        CatalogRequest request,
        String graphName,
        Consumer<GraphStoreWithConfig> removedGraphConsumer,
        boolean failOnMissing
    ) {
        var userCatalogKey = UserCatalog.UserCatalogKey.of(request.databaseName(), graphName);
        var ownCatalog = getUserCatalog(request.username());

        var didRemove = ownCatalog.remove(
            userCatalogKey, removedGraphConsumer,
            failOnMissing && request.restrictSearchToUsernameCatalog()
        );
        if (didRemove || request.restrictSearchToUsernameCatalog()) {
            return;
        }

        var usersWithMatchingGraphs = userCatalogs
            .entrySet()
            .stream()
            .flatMap(e -> Stream.ofNullable(e.getValue().get(userCatalogKey, false)).map(graph -> e.getKey()))
            .collect(Collectors.toSet());

        if (usersWithMatchingGraphs.isEmpty() && failOnMissing) {
            // suggests only own graphs names
            throw ownCatalog.graphNotFoundException(userCatalogKey);
        }

        if (usersWithMatchingGraphs.size() > 1) {
            var usernames = StringJoining.joinVerbose(usersWithMatchingGraphs);
            throw new IllegalArgumentException(formatWithLocale(
                "Multiple graphs that match '%s' are found from the users %s.",
                graphName,
                usernames
            ));
        }

        if (!usersWithMatchingGraphs.isEmpty()) {
            var username = usersWithMatchingGraphs.iterator().next();
            getUserCatalog(username).remove(
                userCatalogKey,
                removedGraphConsumer,
                failOnMissing
            );
        }
    }

    @TestOnly
    public static GraphStoreWithConfig get(String username, DatabaseId databaseId, String graphName) {
        return get(CatalogRequest.of(username, databaseId), graphName);
    }

    @TestOnly
    public static GraphStoreWithConfig get(String username, String databaseName, String graphName) {
        return get(CatalogRequest.of(username, databaseName), graphName);
    }

    public static void set(GraphProjectConfig config, GraphStore graphStore) {
        set(config, graphStore, false);
    }

    public static void overwrite(GraphProjectConfig config, GraphStore graphStore) {
        set(config, graphStore, true);
    }

    private static void set(GraphProjectConfig config, GraphStore graphStore, boolean overwrite) {
        graphStore.canRelease(false);
        userCatalogs.compute(config.username(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(
                UserCatalog.UserCatalogKey.of(graphStore.databaseId(), config.graphName()),
                config,
                graphStore,
                overwrite
            );
            return userCatalog;
        });
    }

    public static boolean exists(String username, String databaseName, String graphName) {
        return getUserCatalog(username).exists(UserCatalog.UserCatalogKey.of(databaseName, graphName));
    }

    public static boolean exists(String username, DatabaseId databaseId, String graphName) {
        return getUserCatalog(username).exists(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static int graphStoresCount() {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.getGraphStores().values().size())
            .sum();
    }

    public static int graphStoresCount(DatabaseId databaseId) {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.getGraphStores(databaseId).values().size())
            .sum();
    }

    public static boolean isEmpty() {
        return graphStoresCount() == 0;
    }

    public static Optional<Map<String, Object>> getDegreeDistribution(
        String username,
        DatabaseId databaseId,
        String graphName
    ) {
        return getUserCatalog(username).getDegreeDistribution(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static void setDegreeDistribution(
        String username,
        DatabaseId databaseId,
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

    public static void removeAllLoadedGraphs(DatabaseId databaseId) {
        userCatalogs.forEach((user, userCatalog) -> userCatalog.remove(databaseId.databaseName()));
    }

    public static Map<GraphProjectConfig, GraphStore> getGraphStores(String username) {
        return getUserCatalog(username).getGraphStores();
    }

    public static Map<GraphProjectConfig, GraphStore> getGraphStores(String username, DatabaseId databaseId) {
        return getUserCatalog(username).getGraphStores(databaseId);
    }

    public static Stream<GraphStoreWithUserNameAndConfig> getAllGraphStores() {
        return userCatalogs
            .entrySet()
            .stream()
            .flatMap(entry -> entry.getValue().streamGraphStores(entry.getKey()));
    }

    private static UserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    @ValueClass
    public interface GraphStoreWithUserNameAndConfig {

        GraphStore graphStore();

        String userName();

        GraphProjectConfig config();
    }

    static class UserCatalog {

        @ValueClass
        public interface UserCatalogKey {

            String graphName();

            String databaseName();

            static UserCatalogKey of(DatabaseId databaseId, String graphName) {
                return of(databaseId.databaseName(), graphName);
            }

            static UserCatalogKey of(String databaseName, String graphName) {
                return ImmutableUserCatalogKey.of(graphName, databaseName);
            }
        }

        private static final UserCatalog EMPTY = new UserCatalog();

        private final Map<UserCatalogKey, GraphStoreWithConfig> graphsByName = new ConcurrentHashMap<>();

        private final Map<UserCatalogKey, Map<String, Object>> degreeDistributionByName = new ConcurrentHashMap<>();

        private void set(
            UserCatalogKey userCatalogKey,
            GraphProjectConfig config,
            GraphStore graphStore,
            boolean overwrite
        ) {
            if (config.graphName() == null || graphStore == null) {
                throw new IllegalArgumentException("Both name and graph store must be not null");
            }
            GraphStoreWithConfig graphStoreWithConfig = GraphStoreWithConfig.of(graphStore, config);

            if (!overwrite && graphsByName.containsKey(userCatalogKey)) {
                throw new IllegalStateException(formatWithLocale(
                    "Graph name %s already loaded",
                    config.graphName()
                ));
            }
            graphsByName.put(userCatalogKey, graphStoreWithConfig);
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

        private @Nullable GraphStoreWithConfig get(UserCatalogKey userCatalogKey, boolean failOnMissing) {
            var graphStoreWithConfig = graphsByName.get(userCatalogKey);

            if (graphStoreWithConfig == null && failOnMissing) {
                throw graphNotFoundException(userCatalogKey);
            }

            return graphStoreWithConfig;
        }

        private NoSuchElementException graphNotFoundException(UserCatalogKey userCatalogKey) {
            var graphName = userCatalogKey.graphName();

            return new NoSuchElementException(
                formatWithLocale(
                    "Graph with name `%s` does not exist on database `%s`. It might exist on another database.",
                    graphName,
                    userCatalogKey.databaseName()
                )
            );
        }

        private Optional<Map<String, Object>> getDegreeDistribution(UserCatalogKey userCatalogKey) {
            if (!graphsByName.containsKey(userCatalogKey)) {
                return Optional.empty();
            }
            return Optional.ofNullable(degreeDistributionByName.get(userCatalogKey));
        }

        private boolean exists(UserCatalogKey userCatalogKey) {
            return userCatalogKey != null && graphsByName.containsKey(userCatalogKey);
        }

        private boolean remove(
            UserCatalogKey userCatalogKey,
            Consumer<GraphStoreWithConfig> removedGraphConsumer,
            boolean failOnMissing
        ) {
            return Optional.ofNullable(get(userCatalogKey, failOnMissing))
                .map(graphStoreWithConfig -> {
                    removedGraphConsumer.accept(graphStoreWithConfig);
                    graphStoreWithConfig.graphStore().canRelease(true);
                    graphStoreWithConfig.graphStore().release();
                    removeDegreeDistribution(userCatalogKey);
                    graphsByName.remove(userCatalogKey);
                    return Boolean.TRUE;
                })
                .orElse(Boolean.FALSE);
        }

        private void remove(String databaseName) {
            graphsByName.keySet().removeIf(userCatalogKey -> userCatalogKey.databaseName().equals(databaseName));
        }

        private Stream<GraphStoreWithUserNameAndConfig> streamGraphStores(String userName) {
            return graphsByName
                .values()
                .stream()
                .map(graphStoreWithConfig -> ImmutableGraphStoreWithUserNameAndConfig.of(
                    graphStoreWithConfig.graphStore(),
                    userName,
                    graphStoreWithConfig.config()
                ));
        }

        private Map<GraphProjectConfig, GraphStore> getGraphStores() {
            return graphsByName.values().stream()
                .collect(Collectors.toMap(
                    GraphStoreWithConfig::config,
                    GraphStoreWithConfig::graphStore
                    )
                );
        }

        private Map<GraphProjectConfig, GraphStore> getGraphStores(DatabaseId databaseId) {
            return graphsByName.entrySet().stream()
                .filter(entry -> entry.getKey().databaseName().equals(databaseId.databaseName()))
                .collect(Collectors.toMap(
                    entry -> entry.getValue().config(),
                    entry -> entry.getValue().graphStore()
                    )
                );
        }
    }

}
