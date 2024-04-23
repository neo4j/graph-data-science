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

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.NoSuchElementException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.extension.GdlSupportPerMethodExtension.DATABASE_ID;

@GdlExtension
class GraphStoreCatalogTest {

    private static final String USER_NAME = "alice";
    private static final String GRAPH_NAME = "graph";
    private static final GraphProjectConfig CONFIG = GraphProjectConfig.emptyWithName(USER_NAME, GRAPH_NAME);

    @GdlGraph
    private static final String TEST_GRAPH = "()";

    @GdlGraph(graphNamePrefix = "other")
    private static final String TEST_GRAPH2 = "()";

    @Inject
    private GraphStore graphStore;

    @Inject
    private GraphStore otherGraphStore;

    @Test
    void set() {
        Assertions.assertFalse(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        assertTrue(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void get() {
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        var graphStoreWithConfig = GraphStoreCatalog.get(
            CatalogRequest.of(USER_NAME, DATABASE_ID),
            GRAPH_NAME
        );
        assertEquals(graphStore, graphStoreWithConfig.graphStore());
        assertEquals(CONFIG, graphStoreWithConfig.config());
    }

    @Test
    void getAsAdminReturnsOtherUsersGraphs() {
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        var graphStoreWithConfig = GraphStoreCatalog.get(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME
        );
        assertEquals(graphStore, graphStoreWithConfig.graphStore());
        assertEquals(CONFIG, graphStoreWithConfig.config());
    }

    @Test
    void getAsAdminReturnsGraphsFromOwnCatalogIfAvailable() {
        var adminConfig = GraphProjectConfig.emptyWithName("admin", GRAPH_NAME);
        GraphStoreCatalog.set(adminConfig, graphStore, ResultStore.EMPTY); // {} -> admin -> DB -> GRAPH_NAME -> graphStore
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);      // {} -> alice -> DB -> GRAPH_NAME -> graphStore

        var graphStoreWithConfig = GraphStoreCatalog.get(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME
        );
        assertEquals(graphStore, graphStoreWithConfig.graphStore());
        assertEquals(adminConfig, graphStoreWithConfig.config());
        assertNotEquals(CONFIG, graphStoreWithConfig.config());
    }

    @Test
    void getAsAdminThrowsIfNoGraphIsMatching() {
        assertThatThrownBy(() -> GraphStoreCatalog.get(CatalogRequest.ofAdmin("admin", DATABASE_ID), GRAPH_NAME))
            .hasMessage("Graph with name `%s` does not exist on database `%s`. It might exist on another database.", GRAPH_NAME, DATABASE_ID.databaseName());
    }

    @Test
    void getAsAdminFailsOnMultipleGraphsMatching() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("alice", GRAPH_NAME), graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);

        assertThatThrownBy(() -> GraphStoreCatalog.get(CatalogRequest.ofAdmin("admin", DATABASE_ID), GRAPH_NAME))
            .hasMessage("Multiple graphs that match '%s' are found from the users alice and bob.", GRAPH_NAME);
    }

    @Test
    void getAsAdminUsesTheUserOverrideToSelectFromMultipleGraphsMatching() {
        var aliceConfig = GraphProjectConfig.emptyWithName("alice", GRAPH_NAME);
        GraphStoreCatalog.set(aliceConfig, graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);

        var graphStoreWithConfig = GraphStoreCatalog.get(
            CatalogRequest.ofAdmin("admin", Optional.of("alice"), DATABASE_ID),
            GRAPH_NAME
        );
        assertEquals(graphStore, graphStoreWithConfig.graphStore());
        assertEquals(aliceConfig, graphStoreWithConfig.config());
    }

    @Test
    void getAsAdminReturnsTheGraphFromTheOverrideEvenIfAGraphFromOwnCatalogIsAvailable() {
        var adminConfig = GraphProjectConfig.emptyWithName("admin", GRAPH_NAME);
        GraphStoreCatalog.set(adminConfig, graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);

        var graphStoreWithConfig = GraphStoreCatalog.get(
            CatalogRequest.ofAdmin("admin", Optional.of(USER_NAME), DATABASE_ID),
            GRAPH_NAME
        );
        assertEquals(graphStore, graphStoreWithConfig.graphStore());
        assertEquals(CONFIG, graphStoreWithConfig.config());
        assertNotEquals(adminConfig, graphStoreWithConfig.config());
    }

    @Test
    void getAsAdminReturnsOnlyTheGraphFromTheOverrideEvenIfAGraphFromOwnCatalogIsAvailable() {
        var adminConfig = GraphProjectConfig.emptyWithName("admin", GRAPH_NAME);
        GraphStoreCatalog.set(adminConfig, graphStore, ResultStore.EMPTY);

        assertThatThrownBy(() -> GraphStoreCatalog.get(
            CatalogRequest.ofAdmin("admin", Optional.of(USER_NAME), DATABASE_ID),
            GRAPH_NAME
        ))
            .hasMessage("Graph with name `%s` does not exist on database `%s`. It might exist on another database.", GRAPH_NAME, DATABASE_ID.databaseName());
    }

    @Test
    void getAsAdminReturnsOnlyTheGraphFromTheOverrideEvenIfAGraphFromADifferentUserIsAvailable() {
        var bobConfig = GraphProjectConfig.emptyWithName("bob", GRAPH_NAME);
        GraphStoreCatalog.set(bobConfig, graphStore, ResultStore.EMPTY);

        assertThatThrownBy(() -> GraphStoreCatalog.get(
            CatalogRequest.ofAdmin("admin", Optional.of(USER_NAME), DATABASE_ID),
            GRAPH_NAME
        ))
            .hasMessage("Graph with name `%s` does not exist on database `%s`. It might exist on another database.", GRAPH_NAME, DATABASE_ID.databaseName());
    }

    @Test
    void remove() {
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        assertTrue(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
        GraphStoreCatalog.remove(
            CatalogRequest.of(USER_NAME, DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {},
            true
        );
        assertFalse(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeAsAdmin() {
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {
                assertEquals(graphStore, graphStoreWithConfig.graphStore());
            },
            true
        );
        assertFalse(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeAsAdminPrefersOwnGraphCatalog() {
        var adminConfig = GraphProjectConfig.emptyWithName("admin", GRAPH_NAME);
        GraphStoreCatalog.set(adminConfig, graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);

        GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {
                assertEquals(graphStore, graphStoreWithConfig.graphStore());
            },
            true
        );
        assertFalse(GraphStoreCatalog.exists("admin", DATABASE_ID, GRAPH_NAME));
        assertTrue(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeAsAdminThrowsIfNoGraphIsMatching() {
        assertThatThrownBy(() -> GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> fail("How did you remove a graph that never existed?"),
            true
        ))
            .hasMessage("Graph with name `%s` does not exist on database `%s`. It might exist on another database.", GRAPH_NAME, DATABASE_ID.databaseName());
    }

    @Test
    void removeAsAdminDoesNotThrowIfNoGraphAreMatchingIfFailIfMissingFlagIsFalse() {
        assertThatCode(() -> GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> fail("How did you remove a graph that never existed?"),
            false
        ))
            .doesNotThrowAnyException();
    }

    @Test
    void removeAsAdminFailsOnMultipleGraphsMatching() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("alice", GRAPH_NAME), graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);

        assertThatThrownBy(() -> GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> fail("Should not have removed the graph"),
            true
        ))
            .hasMessage("Multiple graphs that match '%s' are found from the users alice and bob.", GRAPH_NAME);
    }

    @Test
    void removeAsAdminUsesTheUserOverrideToSelectFromMultipleGraphsMatching() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("alice", GRAPH_NAME), graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);

        GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", Optional.of("alice"), DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {
                assertEquals(graphStore, graphStoreWithConfig.graphStore());
            },
            true
        );
        assertFalse(GraphStoreCatalog.exists("alice", DATABASE_ID, GRAPH_NAME));
        assertTrue(GraphStoreCatalog.exists("bob", DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeDoesNotRemoveOtherUsersGraphs() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);

        GraphStoreCatalog.remove(
            CatalogRequest.of(USER_NAME, DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> fail("Should not have removed the graph"),
            false
        );
        assertTrue(GraphStoreCatalog.exists("bob", DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeAsAdminReturnsTheGraphFromTheOverrideEvenIfAGraphFromOwnCatalogIsAvailable() {
        var adminConfig = GraphProjectConfig.emptyWithName("admin", GRAPH_NAME);
        GraphStoreCatalog.set(adminConfig, graphStore, ResultStore.EMPTY);
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);

        GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", Optional.of(USER_NAME), DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {
                assertEquals(graphStore, graphStoreWithConfig.graphStore());
            },
            true
        );
        assertFalse(GraphStoreCatalog.exists(USER_NAME, DATABASE_ID, GRAPH_NAME));
        assertTrue(GraphStoreCatalog.exists("admin", DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeAsAdminReturnsOnlyTheGraphFromTheOverrideEvenIfAGraphFromOwnCatalogIsAvailable() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("admin", GRAPH_NAME), graphStore, ResultStore.EMPTY);

        assertThatThrownBy(() -> GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", Optional.of(USER_NAME), DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> fail("Should not have removed the graph"),
            true
        ))
            .hasMessage("Graph with name `%s` does not exist on database `%s`. It might exist on another database.", GRAPH_NAME, DATABASE_ID.databaseName());

        assertTrue(GraphStoreCatalog.exists("admin", DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void removeAsAdminReturnsOnlyTheGraphFromTheOverrideEvenIfAGraphFromADifferentUserIsAvailable() {
        var bobConfig = GraphProjectConfig.emptyWithName("bob", GRAPH_NAME);
        GraphStoreCatalog.set(bobConfig, graphStore, ResultStore.EMPTY);

        assertThatThrownBy(() -> GraphStoreCatalog.remove(
            CatalogRequest.ofAdmin("admin", Optional.of(USER_NAME), DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> fail("Should not have removed the graph"),
            true
        ))
            .hasMessage("Graph with name `%s` does not exist on database `%s`. It might exist on another database.", GRAPH_NAME, DATABASE_ID.databaseName());

        assertTrue(GraphStoreCatalog.exists("bob", DATABASE_ID, GRAPH_NAME));
    }

    @Test
    void graphStoresCount() {
        assertEquals(0, GraphStoreCatalog.graphStoreCount(DATABASE_ID));
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        assertEquals(1, GraphStoreCatalog.graphStoreCount(DATABASE_ID));
        GraphStoreCatalog.remove(
            CatalogRequest.of(USER_NAME, DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {},
            true
        );
        assertEquals(0, GraphStoreCatalog.graphStoreCount(DATABASE_ID));
    }

    @Test
    void graphStoresCountAcrossUsers() {
        assertEquals(0, GraphStoreCatalog.graphStoreCount());
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        assertEquals(1, GraphStoreCatalog.graphStoreCount());
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);
        assertEquals(2, GraphStoreCatalog.graphStoreCount());
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("clive", GRAPH_NAME), graphStore, ResultStore.EMPTY);
        assertEquals(3, GraphStoreCatalog.graphStoreCount());
        GraphStoreCatalog.remove(
            CatalogRequest.of(USER_NAME, DATABASE_ID),
            GRAPH_NAME,
            graphStoreWithConfig -> {},
            true
        );
        assertEquals(2, GraphStoreCatalog.graphStoreCount());
    }

    @Test
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        assertEquals(1, GraphStoreCatalog.graphStoreCount(DATABASE_ID));
        GraphStoreCatalog.removeAllLoadedGraphs();
        assertEquals(0, GraphStoreCatalog.graphStoreCount(DATABASE_ID));
    }

    @Test
    void multipleDatabaseIds() {
        var config0 = GraphProjectConfig.emptyWithName(USER_NAME, "graph0");
        var config1 = GraphProjectConfig.emptyWithName(USER_NAME, "graph1");

        DatabaseId databaseId0 = DatabaseId.of("DB_0");
        DatabaseId databaseId1 = DatabaseId.of("DB_1");

        GraphStore graphStore0 = GdlFactory
            .builder()
            .gdlGraph("()-->()")
            .databaseId(databaseId0)
            .build()
            .build();
        GraphStore graphStore1 = GdlFactory
            .builder()
            .gdlGraph("()-->()-->()")
            .databaseId(databaseId1)
            .build()
            .build();

        GraphStoreCatalog.set(config0, graphStore0, ResultStore.EMPTY);
        GraphStoreCatalog.set(config1, graphStore1, ResultStore.EMPTY);

        assertTrue(GraphStoreCatalog.exists(USER_NAME, databaseId0, "graph0"));
        assertFalse(GraphStoreCatalog.exists(USER_NAME, databaseId0, "graph1"));

        assertTrue(GraphStoreCatalog.exists(USER_NAME, databaseId1, "graph1"));
        assertFalse(GraphStoreCatalog.exists(USER_NAME, databaseId1, "graph0"));
    }

    @Test
    void callListeners() {
        var listenerCalls = new MutableInt(0);

        var listener = new GraphStoreCatalogListener() {
            @Override
            public void onProject(String user, String database, String graphName) {
                listenerCalls.increment();
            }
        };

        GraphStoreCatalog.registerListener(listener);
        assertThat(listenerCalls.intValue()).isZero();

        GraphStoreCatalog.set(CONFIG, graphStore, ResultStore.EMPTY);
        assertThat(listenerCalls.intValue()).isEqualTo(1);

        GraphStoreCatalog.unregisterListener(listener);
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("bob", GRAPH_NAME), graphStore, ResultStore.EMPTY);
        assertThat(listenerCalls.intValue()).isEqualTo(1);
    }

    @Test
    void shouldThrowOnMissingGraph() {
        var dummyDatabaseId = DatabaseId.of("mydatabase");

        // test the get code path
        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> GraphStoreCatalog.get(
                CatalogRequest.of(USER_NAME, dummyDatabaseId),
                "myGraph"
            ))
            .withMessage("Graph with name `myGraph` does not exist on database `mydatabase`. It might exist on another database.");

        // test the drop code path
        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> GraphStoreCatalog.remove(
                CatalogRequest.of(USER_NAME, dummyDatabaseId),
                "myGraph",
                graphStoreWithConfig -> {},
                true
            ))
            .withMessage("Graph with name `myGraph` does not exist on database `mydatabase`. It might exist on another database.");
    }
}
