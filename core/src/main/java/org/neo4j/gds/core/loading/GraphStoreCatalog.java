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
import org.neo4j.gds.api.EphemeralResultStore;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreAddedEventListener;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEvent;
import org.neo4j.gds.api.graph.store.catalog.GraphStoreRemovedEventListener;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.utils.ExceptionUtil;
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
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

    private static final Collection<GraphStoreAddedEventListener> graphStoreAddedEventListeners = new HashSet<>();
    private static final Collection<GraphStoreRemovedEventListener> graphStoreRemovedEventListeners = new HashSet<>();

    // as we want to use the Neo4j log if possible and the catalog is a static instance,
    // we make the log injectable
    private static Optional<Log> log = Optional.empty();

    private GraphStoreCatalog() {
    }

    public static void registerGraphStoreAddedListener(GraphStoreAddedEventListener listener) {
        graphStoreAddedEventListeners.add(listener);
    }

    public static void unregisterGraphStoreAddedListener(GraphStoreAddedEventListener listener) {
        graphStoreAddedEventListeners.remove(listener);
    }

    public static void registerGraphStoreRemovedListener(GraphStoreRemovedEventListener listener) {
        graphStoreRemovedEventListeners.add(listener);
    }

    public static void unregisterGraphStoreRemovedListener(GraphStoreRemovedEventListener listener) {
        graphStoreRemovedEventListeners.remove(listener);
    }

    public static void setLog(Log log) {
        GraphStoreCatalog.log = Optional.of(log);
    }

    public static GraphStoreCatalogEntry get(CatalogRequest request, String graphName) {
        var userCatalogKey = UserCatalog.UserCatalogKey.of(request.databaseName(), graphName);
        var ownCatalog = getUserCatalog(request.username());

        var maybeGraph = ownCatalog.get(userCatalogKey, request.restrictSearchToUsernameCatalog());
        if (maybeGraph != null) {
            return maybeGraph;
        }

        var usersWithMatchingGraphs = userCatalogs
            .entrySet()
            .stream()
            .flatMap(
                e -> Stream
                    .ofNullable(e.getValue().get(userCatalogKey, false))
                    .map(graph -> Map.entry(e.getKey(), graph))
            )
            .toList();

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

        throw new IllegalArgumentException(
            formatWithLocale(
                "Multiple graphs that match '%s' are found from the users %s.",
                graphName,
                usernames
            )
        );
    }

    public static void remove(
        CatalogRequest request,
        String graphName,
        Consumer<GraphStoreCatalogEntry> removedGraphConsumer,
        boolean failOnMissing
    ) {
        var userCatalogKey = UserCatalog.UserCatalogKey.of(request.databaseName(), graphName);
        var ownCatalog = getUserCatalog(request.username());

        var didRemove = ownCatalog.remove(
            userCatalogKey,
            removedGraphConsumer,
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
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Multiple graphs that match '%s' are found from the users %s.",
                    graphName,
                    usernames
                )
            );
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
    public static GraphStoreCatalogEntry get(String username, DatabaseId databaseId, String graphName) {
        return get(CatalogRequest.of(username, databaseId), graphName);
    }

    @TestOnly
    public static GraphStoreCatalogEntry get(String username, String databaseName, String graphName) {
        return get(CatalogRequest.of(username, databaseName), graphName);
    }

    public static void set(GraphProjectConfig config, GraphStore graphStore) {
        userCatalogs.compute(config.username(), (user, userCatalog) -> {
            if (userCatalog == null) {
                userCatalog = new UserCatalog();
            }
            userCatalog.set(
                UserCatalog.UserCatalogKey.of(graphStore.databaseInfo().databaseId(), config.graphName()),
                config,
                graphStore
            );
            return userCatalog;
        });

        graphStoreAddedEventListeners.forEach(
            listener -> ExceptionUtil.safeRunWithLogException(
                () -> String.format(
                    Locale.US,
                    "Could not call listener %s on setting the graph %s",
                    listener,
                    config.graphName()
                ),
                () -> listener.onGraphStoreAdded(
                    new GraphStoreAddedEvent(
                        config.username(),
                        graphStore.databaseInfo().databaseId().databaseName(),
                        config.graphName(),
                        MemoryUsage.sizeOf(graphStore)
                    )
                ),
                log.orElseGet(Log::noOpLog)::warn
            )
        );
    }

    public static boolean exists(String username, String databaseName, String graphName) {
        return getUserCatalog(username).exists(UserCatalog.UserCatalogKey.of(databaseName, graphName));
    }

    public static boolean exists(String username, DatabaseId databaseId, String graphName) {
        return getUserCatalog(username).exists(UserCatalog.UserCatalogKey.of(databaseId, graphName));
    }

    public static int graphStoreCount() {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.getGraphStores().size())
            .sum();
    }

    public static int graphStoreCount(DatabaseId databaseId) {
        return userCatalogs
            .values()
            .stream()
            .mapToInt(userCatalog -> userCatalog.getGraphStores(databaseId).size())
            .sum();
    }

    public static boolean isEmpty() {
        return graphStoreCount() == 0;
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

    public static Collection<GraphStoreCatalogEntry> getGraphStores(String username) {
        return getUserCatalog(username).getGraphStores();
    }

    public static Collection<GraphStoreCatalogEntry> getGraphStores(String username, DatabaseId databaseId) {
        return getUserCatalog(username).getGraphStores(databaseId);
    }

    public static Stream<GraphStoreCatalogEntryWithUsername> getAllGraphStores() {
        return userCatalogs
            .entrySet()
            .stream()
            .flatMap(entry -> entry
                .getValue()
                .streamGraphStores(entry.getKey())
            );
    }

    private static UserCatalog getUserCatalog(String username) {
        return userCatalogs.getOrDefault(username, UserCatalog.EMPTY);
    }

    public record GraphStoreCatalogEntryWithUsername(GraphStoreCatalogEntry catalogEntry, String username) {}

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

        private final Map<UserCatalogKey, GraphStoreCatalogEntry> graphsByName = new ConcurrentHashMap<>();

        private final Map<UserCatalogKey, Map<String, Object>> degreeDistributionByName = new ConcurrentHashMap<>();

        private void set(
            UserCatalogKey userCatalogKey,
            GraphProjectConfig config,
            GraphStore graphStore
        ) {
            if (config.graphName() == null || graphStore == null) {
                throw new IllegalArgumentException("Both name and graph store must be not null");
            }

            GraphStoreCatalogEntry graphStoreCatalogEntry = new GraphStoreCatalogEntry(graphStore, config, new EphemeralResultStore());

            if (graphsByName.containsKey(userCatalogKey)) {
                throw new IllegalStateException(
                    formatWithLocale(
                        "Graph name %s already loaded",
                        config.graphName()
                    )
                );
            }
            graphsByName.put(userCatalogKey, graphStoreCatalogEntry);
        }

        private void setDegreeDistribution(UserCatalogKey userCatalogKey, Map<String, Object> degreeDistribution) {

            if (userCatalogKey == null || degreeDistribution == null) {
                throw new IllegalArgumentException("Both name and degreeDistribution must be not null");
            }
            if (!graphsByName.containsKey(userCatalogKey)) {
                throw new IllegalArgumentException(
                    formatWithLocale(
                        "Cannot set degreeDistribution because graph %s does not exist",
                        userCatalogKey.graphName()
                    )
                );
            }
            degreeDistributionByName.put(userCatalogKey, degreeDistribution);
        }

        private void removeDegreeDistribution(UserCatalogKey userCatalogKey) {
            degreeDistributionByName.remove(userCatalogKey);
        }

        private @Nullable GraphStoreCatalogEntry get(UserCatalogKey userCatalogKey, boolean failOnMissing) {
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
            Consumer<GraphStoreCatalogEntry> removedGraphConsumer,
            boolean failOnMissing
        ) {
            return Optional.ofNullable(get(userCatalogKey, failOnMissing))
                .map(graphStoreWithConfig -> {
                    removedGraphConsumer.accept(graphStoreWithConfig);
                    removeDegreeDistribution(userCatalogKey);
                    var removed = graphsByName.remove(userCatalogKey);
                    var config = removed.config();
                    var graphStore = removed.graphStore();

                    graphStoreRemovedEventListeners.forEach(
                        listener -> ExceptionUtil.safeRunWithLogException(
                            () -> String.format(
                                Locale.US,
                                "Could not call listener %s on setting the graph %s",
                                listener,
                                config.graphName()
                            ),
                            () -> listener.onGraphStoreRemoved(
                                new GraphStoreRemovedEvent(
                                    config.username(),
                                    graphStore.databaseInfo().databaseId().databaseName(),
                                    config.graphName(),
                                    MemoryUsage.sizeOf(graphStore)
                                )
                            ),
                            log.orElseGet(Log::noOpLog)::warn
                        )
                    );


                    return Boolean.TRUE;
                })
                .orElse(Boolean.FALSE);
        }

        private void remove(String databaseName) {
            graphsByName.keySet().removeIf(userCatalogKey -> userCatalogKey.databaseName().equals(databaseName));
        }

        private Stream<GraphStoreCatalogEntryWithUsername> streamGraphStores(String userName) {
            return graphsByName
                .values()
                .stream()
                .map(catalogEntry -> new GraphStoreCatalogEntryWithUsername(catalogEntry, userName));
        }

        private Collection<GraphStoreCatalogEntry> getGraphStores() {
            return graphsByName.values();
        }

        private Collection<GraphStoreCatalogEntry> getGraphStores(DatabaseId databaseId) {
            return graphsByName.entrySet()
                .stream()
                .filter(entry -> entry.getKey().databaseName().equals(databaseId.databaseName()))
                .map(Map.Entry::getValue)
                .toList();
        }
    }

}
