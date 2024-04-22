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
package org.neo4j.gds.applications.graphstorecatalog;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.CatalogRequest;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DropGraphApplicationTest {
    @Test
    void shouldDropGraphs() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphApplication(graphStoreCatalogService);

        var catalogRequest = CatalogRequest.of("some user", DatabaseId.of("some database"));
        var graphStoreWithConfig1 = mock(GraphStoreCatalogEntry.class);
        var graphStoreWithConfig2 = mock(GraphStoreCatalogEntry.class);
        when(graphStoreCatalogService.removeGraph(catalogRequest, GraphName.parse("foo"), false)).thenReturn(
            graphStoreWithConfig1);
        when(graphStoreCatalogService.removeGraph(catalogRequest, GraphName.parse("bar"), false)).thenReturn(
            graphStoreWithConfig2);
        var results = dropGraphService.compute(
            List.of(GraphName.parse("foo"), GraphName.parse("bar")),
            false,
            DatabaseId.of("some database"),
            new User("some user", false),
            Optional.empty()
        );

        assertThat(results).containsExactly(graphStoreWithConfig1, graphStoreWithConfig2);
    }

    @Test
    void shouldValidateGraphsExistBeforeRemovingAnyOhAndListThemAllInOneGo() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphApplication(graphStoreCatalogService);

        var request = CatalogRequest.of("some user", DatabaseId.of("some database"));
        var g1 = GraphName.parse("foo");
        var g2 = GraphName.parse("bar");
        var g3 = GraphName.parse("baz");
        var g4 = GraphName.parse("quux");
        when(graphStoreCatalogService.get(request, g1)).thenReturn(mock(GraphStoreCatalogEntry.class));
        when(graphStoreCatalogService.get(request, g2)).thenThrow(new NoSuchElementException("aha!"));
        when(graphStoreCatalogService.get(request, g3)).thenReturn(mock(GraphStoreCatalogEntry.class));
        when(graphStoreCatalogService.get(request, g4)).thenThrow(new NoSuchElementException("another!"));
        try {
            dropGraphService.compute(
                List.of(g1, g2, g3, g4),
                true,
                DatabaseId.of("some database"),
                new User("some user", false),
                Optional.empty()
            );
            fail();
        } catch (NoSuchElementException e) {
            assertThat(e.getMessage()).isEqualTo(
                "The graphs `bar`, and `quux` do not exist on database `some database`.");
        }

        verify(graphStoreCatalogService).get(request, g1);
        verify(graphStoreCatalogService).get(request, g2);
        verify(graphStoreCatalogService).get(request, g3);
        verify(graphStoreCatalogService).get(request, g4);
        verifyNoMoreInteractions(graphStoreCatalogService);
    }

    @Test
    void shouldRespectFailIfMissingFlag() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphApplication(graphStoreCatalogService);

        var request = CatalogRequest.of("some user", DatabaseId.of("some database"));
        var graphStoreWithConfig1 = mock(GraphStoreCatalogEntry.class);
        var graphStoreWithConfig2 = mock(GraphStoreCatalogEntry.class);
        var g1 = GraphName.parse("foo");
        var g2 = GraphName.parse("bar");
        var g3 = GraphName.parse("baz");
        var g4 = GraphName.parse("quux");
        when(graphStoreCatalogService.removeGraph(request, g1, false)).thenReturn(graphStoreWithConfig1);
        when(graphStoreCatalogService.removeGraph(request, g2, false)).thenReturn(null);
        when(graphStoreCatalogService.removeGraph(request, g3, false)).thenReturn(graphStoreWithConfig2);
        when(graphStoreCatalogService.removeGraph(request, g4, false)).thenReturn(null);
        var results = dropGraphService.compute(
            List.of(g1, g2, g3, g4),
            false,
            DatabaseId.of("some database"),
            new User("some user", false),
            Optional.empty()
        );

        assertThat(results).containsExactly(graphStoreWithConfig1, graphStoreWithConfig2);
    }

    @Test
    void shouldNotAllowUsernameOverrideForNonAdmins() {
        var service = new DropGraphApplication(null);

        try {
            service.compute(
                List.of(GraphName.parse("some graph")),
                false,
                DatabaseId.of("some database"),
                new User("some user", false),
                Optional.of("some other user")
            );

            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("Cannot override the username as a non-admin");
        }
    }
}
