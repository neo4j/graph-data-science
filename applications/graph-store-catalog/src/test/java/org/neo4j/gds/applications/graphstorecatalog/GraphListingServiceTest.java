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
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.GraphStoreCatalogEntry;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GraphListingServiceTest {
    @Test
    void shouldListAllGraphsForAdmins() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var graphListingService = new GraphListingService(graphStoreCatalogService);

        var graphStore1 = stubCatalogEntryWithUsername();
        var graphStore2 = stubCatalogEntryWithUsername();
        var graphStore3 = stubCatalogEntryWithUsername();
        when(graphStoreCatalogService.getAllGraphStores()).thenReturn(Stream.of(
            graphStore1,
            graphStore2,
            graphStore3
        ));
        var result = graphListingService.listGraphs(new User("Bossman", true));

        assertThat(result).containsExactly(
            new GraphStoreCatalogEntry(graphStore1.graphStore(), graphStore1.config()),
            new GraphStoreCatalogEntry(graphStore2.graphStore(), graphStore2.config()),
            new GraphStoreCatalogEntry(graphStore3.graphStore(), graphStore3.config())
        );
    }

    @Test
    void shouldOnlyListOwnGraphsForNonAdmins() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var graphListingService = new GraphListingService(graphStoreCatalogService);

        var graphStore1 = stubCatalogEntryWithUsername();
        var graphStore2 = stubCatalogEntryWithUsername();
        var graphStore3 = stubCatalogEntryWithUsername();
        when(graphStoreCatalogService.getGraphStores(new User("nobody", false))).thenReturn(Map.of(
            graphStore1.config(), graphStore1.graphStore(),
            graphStore2.config(), graphStore2.graphStore(),
            graphStore3.config(), graphStore3.graphStore()
        ));
        var result = graphListingService.listGraphs(new User("nobody", false));

        assertThat(result).containsExactlyInAnyOrder(
            new GraphStoreCatalogEntry(graphStore1.graphStore(), graphStore1.config()),
            new GraphStoreCatalogEntry(graphStore2.graphStore(), graphStore2.config()),
            new GraphStoreCatalogEntry(graphStore3.graphStore(), graphStore3.config())
        );
    }

    private static GraphStoreCatalogEntry stubCatalogEntryWithUsername() {
        return new GraphStoreCatalogEntry(new StubGraphStore(), new StubGraphProjectConfig());
    }
}
