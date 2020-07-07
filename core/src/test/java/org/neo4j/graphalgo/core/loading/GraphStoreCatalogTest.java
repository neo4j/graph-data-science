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

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestSupport;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@GdlExtension
class GraphStoreCatalogTest {

    private static final String USER_NAME = "";
    private static final String GRAPH_NAME = "graph";
    private static final GraphCreateFromStoreConfig CONFIG =
        GraphCreateFromStoreConfig.emptyWithName(USER_NAME, GRAPH_NAME);

    @GdlGraph
    private static final String TEST_GRAPH = "()";

    @Inject
    private GraphStore graphStore;

    @Test
    void set() {
        assertFalse(GraphStoreCatalog.exists(USER_NAME, GRAPH_NAME));
        GraphStoreCatalog.set(CONFIG, graphStore);
        assertTrue(GraphStoreCatalog.exists(USER_NAME, GRAPH_NAME));
    }

    @Test
    void get() {
        GraphStoreCatalog.set(CONFIG, graphStore);
        GraphStoreWithConfig graphStoreWithConfig = GraphStoreCatalog.get(USER_NAME, GRAPH_NAME);
        assertEquals(graphStore, graphStoreWithConfig.graphStore());
        assertEquals(CONFIG, graphStoreWithConfig.config());
    }

    @Test
    void remove() {
        GraphStoreCatalog.set(CONFIG, graphStore);
        assertTrue(GraphStoreCatalog.exists(USER_NAME, GRAPH_NAME));
        GraphStoreCatalog.remove(USER_NAME, GRAPH_NAME, graphStoreWithConfig -> {});
        assertFalse(GraphStoreCatalog.exists(USER_NAME, GRAPH_NAME));
    }

    @Test
    void graphStoresCount() {
        assertEquals(0, GraphStoreCatalog.graphStoresCount());
        GraphStoreCatalog.set(CONFIG, graphStore);
        assertEquals(1, GraphStoreCatalog.graphStoresCount());
        GraphStoreCatalog.remove(USER_NAME, GRAPH_NAME, graphStoreWithConfig -> {});
        assertEquals(0, GraphStoreCatalog.graphStoresCount());
    }

    @Test
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.set(CONFIG, graphStore);
        assertEquals(1, GraphStoreCatalog.graphStoresCount());
        GraphStoreCatalog.removeAllLoadedGraphs();
        assertEquals(0, GraphStoreCatalog.graphStoresCount());
    }

    @Test
    void getUnion() {
        GraphStoreCatalog.set(CONFIG, graphStore);
        var actual = GraphStoreCatalog.getUnion(USER_NAME, GRAPH_NAME).get();
        TestSupport.assertGraphEquals(graphStore.getUnion(), actual);
    }
}