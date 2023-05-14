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
package org.neo4j.gds.catalog;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.TestMethodRunner;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.utils.GdsFeatureToggles;

import static org.assertj.core.api.Assertions.assertThat;

class GraphMemoryUsageTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
    void shouldContainAdjacencyListMemoryInfo(TestMethodRunner runner) {
        runner.run(() -> {
            var gdlFactory = GdlFactory.of("()-[:R1]->()");

            var config = gdlFactory.graphProjectConfig();
            var graphStore = gdlFactory.build();

            var graphStoreWithConfig = GraphStoreWithConfig.of(graphStore, config);
            var graphMemoryUsage = GraphMemoryUsageProc.GraphMemoryUsage.of(graphStoreWithConfig);

            assertThat(graphMemoryUsage.detailSizeInBytes.get("adjacencyLists"))
                .asInstanceOf(InstanceOfAssertFactories.MAP)
                .hasEntrySatisfying(
                    "R1",
                    (inner) -> assertThat(inner)
                        .asInstanceOf(InstanceOfAssertFactories.MAP)
                        .hasEntrySatisfying(
                            "bytesTotal",
                            (bytes) -> assertThat(bytes)
                                .asInstanceOf(InstanceOfAssertFactories.LONG)
                                .isGreaterThan(0L)
                        )
                        .hasEntrySatisfying(
                            "bytesOnHeap",
                            (bytes) -> assertThat(bytes)
                                .asInstanceOf(InstanceOfAssertFactories.LONG)
                                .isGreaterThan(0L)
                        )
                        .hasEntrySatisfying(
                            "bytesOffHeap",
                            (bytes) -> assertThat(bytes)
                                .asInstanceOf(InstanceOfAssertFactories.LONG)
                                .satisfies((value) -> {
                                    if (GdsFeatureToggles.USE_PACKED_ADJACENCY_LIST.isEnabled()) {
                                        assertThat(value).isGreaterThan(0L);
                                    } else {
                                        assertThat(value).isEqualTo(0L);
                                    }
                                })
                        )
                );
        });
    }
}
