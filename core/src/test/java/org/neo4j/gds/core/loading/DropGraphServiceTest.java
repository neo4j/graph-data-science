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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.User;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DropGraphServiceTest {
    @Test
    void shouldDropGraphs() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphService(graphStoreCatalogService);

        var catalogRequest = CatalogRequest.of("some user", DatabaseId.from("some database"));
        var graphStoreWithConfig1 = mock(GraphStoreWithConfig.class);
        var graphStoreWithConfig2 = mock(GraphStoreWithConfig.class);
        when(graphStoreCatalogService.removeGraph(catalogRequest, "foo", false)).thenReturn(graphStoreWithConfig1);
        when(graphStoreCatalogService.removeGraph(catalogRequest, "bar", false)).thenReturn(graphStoreWithConfig2);
        List<GraphStoreWithConfig> results = dropGraphService.compute(
            List.of("foo", "bar"),
            false,
            DatabaseId.from("some database"),
            new User("some user", false),
            Optional.empty()
        );

        assertThat(results).containsExactly(graphStoreWithConfig1, graphStoreWithConfig2);
    }

    @Test
    void shouldValidateGraphsExistBeforeRemovingAnyOhAndListThemAllInOneGo() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphService(graphStoreCatalogService);

        var request = CatalogRequest.of("some user", DatabaseId.from("some database"));
        when(graphStoreCatalogService.get(request, "foo")).thenReturn(mock(GraphStoreWithConfig.class));
        when(graphStoreCatalogService.get(request, "bar")).thenThrow(new NoSuchElementException("aha!"));
        when(graphStoreCatalogService.get(request, "baz")).thenReturn(mock(GraphStoreWithConfig.class));
        when(graphStoreCatalogService.get(request, "quux")).thenThrow(new NoSuchElementException("another!"));
        try {
            dropGraphService.compute(
                List.of("foo", "bar", "baz", "quux"),
                true,
                DatabaseId.from("some database"),
                new User("some user", false),
                Optional.empty()
            );
            fail();
        } catch (NoSuchElementException e) {
            assertThat(e.getMessage()).isEqualTo(
                "The graphs `bar`, and `quux` do not exist on database `some database`.");
        }

        verify(graphStoreCatalogService).get(request, "foo");
        verify(graphStoreCatalogService).get(request, "bar");
        verify(graphStoreCatalogService).get(request, "baz");
        verify(graphStoreCatalogService).get(request, "quux");
        verifyNoMoreInteractions(graphStoreCatalogService);
    }

    @Test
    void shouldRespectFailIfMissingFlag() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphService(graphStoreCatalogService);

        var request = CatalogRequest.of("some user", DatabaseId.from("some database"));
        var graphStoreWithConfig1 = mock(GraphStoreWithConfig.class);
        var graphStoreWithConfig2 = mock(GraphStoreWithConfig.class);
        when(graphStoreCatalogService.removeGraph(request, "foo", false)).thenReturn(graphStoreWithConfig1);
        when(graphStoreCatalogService.removeGraph(request, "bar", false)).thenReturn(null);
        when(graphStoreCatalogService.removeGraph(request, "baz", false)).thenReturn(graphStoreWithConfig2);
        when(graphStoreCatalogService.removeGraph(request, "quux", false)).thenReturn(null);
        var results = dropGraphService.compute(
            List.of("foo", "bar", "baz", "quux"),
            false,
            DatabaseId.from("some database"),
            new User("some user", false),
            Optional.empty()
        );

        assertThat(results).containsExactly(graphStoreWithConfig1, graphStoreWithConfig2);
    }

    @Test
    void shouldNotAllowUsernameOverrideForNonAdmins() {
        var service = new DropGraphService(null);

        try {
            service.compute(
                List.of("some graph"),
                false,
                DatabaseId.from("some database"),
                new User("some user", false),
                Optional.of("some other user")
            );

            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).isEqualTo("Cannot override the username as a non-admin");
        }
    }
}
