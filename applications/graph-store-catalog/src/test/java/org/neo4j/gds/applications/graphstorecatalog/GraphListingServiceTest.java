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

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog.GraphStoreWithUserNameAndConfig;
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

        var graphStore1 = new StubGraphStoreWithUserNameAndConfig();
        var graphStore2 = new StubGraphStoreWithUserNameAndConfig();
        var graphStore3 = new StubGraphStoreWithUserNameAndConfig();
        when(graphStoreCatalogService.getAllGraphStores()).thenReturn(Stream.of(
            graphStore1,
            graphStore2,
            graphStore3
        ));
        var result = graphListingService.listGraphs(new User("Bossman", true));

        assertThat(result).containsExactly(
            Pair.of(graphStore1.config(), graphStore1.graphStore()),
            Pair.of(graphStore2.config(), graphStore2.graphStore()),
            Pair.of(graphStore3.config(), graphStore3.graphStore())
        );
    }

    @Test
    void shouldOnlyListOwnGraphsForNonAdmins() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var graphListingService = new GraphListingService(graphStoreCatalogService);

        var graphStore1 = new StubGraphStoreWithUserNameAndConfig();
        var graphStore2 = new StubGraphStoreWithUserNameAndConfig();
        var graphStore3 = new StubGraphStoreWithUserNameAndConfig();
        when(graphStoreCatalogService.getGraphStores(new User("nobody", false))).thenReturn(Map.of(
            graphStore1.config(), graphStore1.graphStore(),
            graphStore2.config(), graphStore2.graphStore(),
            graphStore3.config(), graphStore3.graphStore()
        ));
        var result = graphListingService.listGraphs(new User("nobody", false));

        assertThat(result).containsExactlyInAnyOrder(
            Pair.of(graphStore1.config(), graphStore1.graphStore()),
            Pair.of(graphStore2.config(), graphStore2.graphStore()),
            Pair.of(graphStore3.config(), graphStore3.graphStore())
        );
    }

    /*
     * Look, this is what it is. Just enough stubbing to be able to assert that we get stuff passing through things.
     * It does not matter what's inside these POJOs.
     * I wish we didn't have these modeled this way: strongly typed holders of other strong types.
     *  Makes me have to create these nested stubs :shrug:
     */
    private static class StubGraphStoreWithUserNameAndConfig implements GraphStoreWithUserNameAndConfig {
        private final GraphStore graphStore = new StubGraphStore();
        private final GraphProjectConfig graphProjectConfig = new StubGraphProjectConfig();

        @Override
        public GraphStore graphStore() {
            return graphStore;
        }

        @Override
        public String userName() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public GraphProjectConfig config() {
            return graphProjectConfig;
        }
    }
}
