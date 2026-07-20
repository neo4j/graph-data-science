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
package org.neo4j.gds.mem;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.GdlGraphStoreBuilder;
import org.neo4j.gds.GdlSupport;
import org.neo4j.gds.core.TestMethodRunner;
import org.neo4j.gds.core.loading.ArrayIdMapBuilder;
import org.neo4j.gds.utils.GdsFeatureToggles;

import static org.assertj.core.api.Assertions.assertThat;

class GraphStoreMemoryUsageTest {

    @Test
    void shouldContainArrayIdMapInformation() {
        var graphStore = new GdlGraphStoreBuilder()
            .gdl("(a)-[r]->(b)")
            .idMapBuilderType(ArrayIdMapBuilder.ID)
            .build();

        var graphMemoryUsage = GraphStoreMemoryUsageFactory.of(graphStore);

        assertThat(graphMemoryUsage.sizeInBytes()).isGreaterThan(0L);
        assertThat(graphMemoryUsage.detailSizeInBytes().get("nodes"))
            .asInstanceOf(InstanceOfAssertFactories.MAP)
            .hasEntrySatisfying(
                "total",
                total -> assertThat(total)
                    .asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("total")
                    .isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "mapping",
                total -> assertThat(total)
                    .asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("mapping")
                    .isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "forwardMapping",
                fwMapping -> assertThat(fwMapping)
                    .asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("forwardMapping")
                    .isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "backwardMapping",
                bwMapping -> assertThat(bwMapping)
                    .asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("backwardMapping")
                    .isGreaterThan(0L)
            );
    }

    @Test
    void shouldContainAdjacencyListInformation() {
        var graphStore = GdlSupport.graphStoreFromGDL("()-[:R1]->()");

        var graphMemoryUsage = GraphStoreMemoryUsageFactory.of(graphStore);

        assertThat(graphMemoryUsage.sizeInBytes()).isGreaterThan(0L);
        assertThat(graphMemoryUsage.detailSizeInBytes().get("relationships"))
            .asInstanceOf(InstanceOfAssertFactories.MAP)
            .hasEntrySatisfying(
                "total",
                total -> assertThat(total).asInstanceOf(InstanceOfAssertFactories.LONG).as("total").isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "adjacencyLists",
                adjLists -> assertThat(adjLists).asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("adjacencyLists")
                    .isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "degrees",
                degrees -> assertThat(degrees).asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("degrees")
                    .isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "offsets",
                offsets -> assertThat(offsets).asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("degrees")
                    .isGreaterThan(0L)
            )
            .hasEntrySatisfying(
                "targetIds",
                targetIds -> assertThat(targetIds).asInstanceOf(InstanceOfAssertFactories.LONG)
                    .as("targetIds")
                    .isGreaterThan(0L)
            );
    }

    @Nested
    class VariableCompressionTest {
        @ParameterizedTest
        @MethodSource("org.neo4j.gds.core.TestMethodRunner#adjacencyCompressions")
        void shouldContainAdjacencyListMemoryInfo(TestMethodRunner runner) {
            runner.run(() -> {
                var graphStore = GdlSupport.graphStoreFromGDL("()-[:R1]->()");
                var graphMemoryUsage = GraphStoreMemoryUsageFactory.of(graphStore);
                assertThat(graphMemoryUsage.detailSizeInBytes().get("adjacencyLists"))
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
                                            assertThat(value).isGreaterThanOrEqualTo(0L);
                                        } else {
                                            assertThat(value).isEqualTo(0L);
                                        }
                                    })
                            )
                    );
            });
        }
    }
}
