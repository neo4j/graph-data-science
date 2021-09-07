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
package org.neo4j.gds.core.cypher;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphCreateFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class CypherGraphStoreCatalogHelperTest {

    @GdlGraph(graphNamePrefix = "a")
    public static String DB_CYPHER_A = "CREATE (:A)";

    @GdlGraph(graphNamePrefix = "b")
    public static String DB_CYPHER_B = "CREATE (:B)";

    @Inject
    GraphStore aGraphStore;

    @Inject
    GraphStore bGraphStore;

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldSetWrapperForExistingGraphStore() {
        var graphStoreWrapper = new TestGraphStoreWrapper(aGraphStore);
        var graphName = "testGraph";
        var config = GraphCreateFromStoreConfig.emptyWithName("", graphName);
        GraphStoreCatalog.set(config, aGraphStore);
        CypherGraphStoreCatalogHelper.setWrappedGraphStore(config, graphStoreWrapper);
        assertThat(graphStoreWrapper).isSameAs(GraphStoreCatalog.get("", aGraphStore.databaseId(), graphName).graphStore());
    }

    @Test
    void shouldFailWhenSettingIncompatibleWrapper() {
        var graphStoreWrapper = new TestGraphStoreWrapper(bGraphStore);
        var graphName = "testGraph";
        var config = GraphCreateFromStoreConfig.emptyWithName("", graphName);
        GraphStoreCatalog.set(config, aGraphStore);
        assertThatThrownBy(() -> CypherGraphStoreCatalogHelper.setWrappedGraphStore(config, graphStoreWrapper))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("incompatible graph store wrapper");
    }

    static class TestGraphStoreWrapper extends GraphStoreAdapter {
        public TestGraphStoreWrapper(GraphStore graphStore) {
            super(graphStore);
        }
    }
}
