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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.DatabaseInfo.DatabaseLocation;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphStoreCatalogServiceTest {
    /**
     * We need this because GraphStoreCatalog is all static fields :grimacing:
     */
    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void shouldDropGraphFromCatalog() {
        var configuration = GraphProjectConfig.emptyWithName("some user", "some graph");
        // we _could_ write a stub for GraphStore; this is good enough for now tho
        var graphStore = mock(GraphStore.class);
        when(graphStore.databaseInfo()).thenReturn(
            DatabaseInfo.of(
                DatabaseId.of("some database"),
                DatabaseLocation.LOCAL
            )
        );
        GraphStoreCatalog.set(configuration, graphStore, ResultStore.EMPTY); // shorthand for project
        var service = new GraphStoreCatalogService();

        assertTrue(
            service.graphExists(
                new User("some user", false),
                DatabaseId.of("some database"),
                GraphName.parse("some graph")
            )
        );

        var graphStoreWithConfig = service.removeGraph(
            CatalogRequest.of("some user", "some database"),
            GraphName.parse("some graph"),
            true
        );
        assertSame(configuration, graphStoreWithConfig.config());
        assertSame(graphStore, graphStoreWithConfig.graphStore());

        assertFalse(
            service.graphExists(
                new User("some user", false),
                DatabaseId.of("some database"),
                GraphName.parse("some graph")
            )
        );
    }

    @Test
    void shouldRespectFailFlag() {
        var service = new GraphStoreCatalogService();

        assertNull(
            service.removeGraph(
                CatalogRequest.of("some user", "some database"),
                GraphName.parse("some graph"),
                false
            )
        );

        try {
            service.removeGraph(
                CatalogRequest.of("some user", "some database"),
                GraphName.parse("some graph"),
                true
            );

            fail();
        } catch (NoSuchElementException e) {
            assertThat(e.getMessage()).isEqualTo(
                "Graph with name `some graph` does not exist on database `some database`. It might exist on another database."
            );
        }
    }

}
