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
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.DatabaseInfo;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DegreeDistributionApplierTest {
    @Test
    void shouldNotApplyDegreeDistributions() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var degreeDistributionApplier = new DegreeDistributionApplier(graphStoreCatalogService, null);

        var config1 = new StubGraphProjectConfig();
        var config2 = new StubGraphProjectConfig();
        var graphStore1 = new StubGraphStore();
        var graphStore2 = new StubGraphStore();
        List<Pair<GraphStoreWithConfig, Map<String, Object>>> result = degreeDistributionApplier.process(
            List.of(
                Pair.of(config1, graphStore1),
                Pair.of(config2, graphStore2)
            ),
            false,
            null
        );

        assertThat(result).containsExactly(
            Pair.of(new GraphStoreWithConfig(graphStore1, config1), null),
            Pair.of(new GraphStoreWithConfig(graphStore2, config2), null)
        );
    }

    @Test
    void shouldApplyAndCacheDegreeDistributions() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var degreeDistributionService = mock(DegreeDistributionService.class);
        var degreeDistributionApplier = new DegreeDistributionApplier(
            graphStoreCatalogService,
            degreeDistributionService
        );

        var config1 = new StubGraphProjectConfig("Alice", "g1");
        var config2 = new StubGraphProjectConfig("Bob", "g2");
        var graphStore1 = new StubGraphStore(
            DatabaseInfo.of(
                DatabaseId.of("db1"),
                DatabaseInfo.DatabaseLocation.LOCAL
            )
        );
        var graphStore2 = new StubGraphStore(
            DatabaseInfo.of(
                DatabaseId.of("db2"),
                DatabaseInfo.DatabaseLocation.LOCAL
            )
        );
        when(degreeDistributionService.compute(graphStore1, null)).thenReturn(Map.of("some", 42));
        when(degreeDistributionService.compute(graphStore2, null)).thenReturn(Map.of("degdist", 87));
        List<Pair<GraphStoreWithConfig, Map<String, Object>>> result = degreeDistributionApplier.process(
            List.of(
                Pair.of(config1, graphStore1),
                Pair.of(config2, graphStore2)
            ),
            true,
            null
        );

        assertThat(result).containsExactly(
            Pair.of(new GraphStoreWithConfig(graphStore1, config1), Map.of("some", 42)),
            Pair.of(new GraphStoreWithConfig(graphStore2, config2), Map.of("degdist", 87))
        );

        // the caching bit
        verify(graphStoreCatalogService).setDegreeDistribution(
            new User("Alice", false),
            DatabaseId.of("db1"),
            GraphName.parse("g1"),
            Map.of("some", 42)
        );
        verify(graphStoreCatalogService).setDegreeDistribution(
            new User("Bob", false),
            DatabaseId.of("db2"),
            GraphName.parse("g2"),
            Map.of("degdist", 87)
        );
    }

    @Test
    void shouldUseCachedDegreeDistributions() {
        var graphStoreCatalogService = mock(GraphStoreCatalogService.class);
        var degreeDistributionApplier = new DegreeDistributionApplier(graphStoreCatalogService, null);

        var config1 = new StubGraphProjectConfig("Alice", "g1");
        var config2 = new StubGraphProjectConfig("Bob", "g2");
        var graphStore1 = new StubGraphStore(
            DatabaseInfo.of(DatabaseId.of("db1"), DatabaseInfo.DatabaseLocation.LOCAL)
        );
        var graphStore2 = new StubGraphStore(
            DatabaseInfo.of(DatabaseId.of("db2"), DatabaseInfo.DatabaseLocation.LOCAL)
        );
        when(
            graphStoreCatalogService.getDegreeDistribution(
                new User("Alice", false),
                DatabaseId.of("db1"),
                GraphName.parse("g1")
            )
        ).thenReturn(Optional.of(Map.of("dd1", 7, "dd2", 11)));
        when(
            graphStoreCatalogService.getDegreeDistribution(
                new User("Bob", false),
                DatabaseId.of("db2"),
                GraphName.parse("g2")
            )
        ).thenReturn(Optional.of(Map.of("dd1", 512, "dd2", 1024)));
        List<Pair<GraphStoreWithConfig, Map<String, Object>>> result = degreeDistributionApplier.process(
            List.of(
                Pair.of(config1, graphStore1),
                Pair.of(config2, graphStore2)
            ),
            true,
            null
        );

        assertThat(result).containsExactly(
            Pair.of(new GraphStoreWithConfig(graphStore1, config1), Map.of("dd1", 7, "dd2", 11)),
            Pair.of(new GraphStoreWithConfig(graphStore2, config2), Map.of("dd1", 512, "dd2", 1024))
        );
    }
}
