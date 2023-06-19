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
import static org.mockito.Mockito.when;

class DropGraphServiceTest {
    @Test
    void shouldDropGraph() {
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
    void shouldValidateGraphsExist() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var dropGraphService = new DropGraphService(graphStoreCatalogService);

        var request = CatalogRequest.of("some user", DatabaseId.from("some database"));
        when(graphStoreCatalogService.get(request, "foo")).thenThrow(new NoSuchElementException("aha!"));
        when(graphStoreCatalogService.get(request, "bar")).thenReturn(mock(GraphStoreWithConfig.class));
        when(graphStoreCatalogService.get(request, "baz")).thenThrow(new NoSuchElementException("aha!"));
        try {
            dropGraphService.compute(
                List.of("foo", "bar", "baz"),
                true,
                DatabaseId.from("some database"),
                new User("some user", false),
                Optional.empty()
            );
            fail();
        } catch (NoSuchElementException e) {
            assertThat(e.getMessage()).isEqualTo("The graphs `foo`, and `baz` do not exist on database `some database`.");
        }
    }
}
