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
package org.neo4j.gds.spanningtree;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.gdl.GdlFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpanningTreeAlgorithmFactoryTest {
    @Test
    void shouldThrowIfNotUndirected() {
        var graph = GdlFactory.of("(a)-[:foo{cost:1.0}]->(b)").build().getUnion();
        var config = SpanningTreeStatsConfigImpl.builder().sourceNode(0).relationshipWeightProperty("cost").build();
        var spanningTreeAlgorithmFactory = new SpanningTreeAlgorithmFactory<>();
        assertThatThrownBy(() -> {
            spanningTreeAlgorithmFactory.build(
                graph,
                config,
                ProgressTracker.NULL_TRACKER
            );
        }).hasMessageContaining(
            "undirected");
    }


    @Test
    void shouldEstimateMemory() {
        var config = SpanningTreeStatsConfigImpl.builder().sourceNode(0).relationshipWeightProperty("foo").build();
        var estimate = new SpanningTreeAlgorithmFactory<>().memoryEstimation(config)
            .estimate(
                GraphDimensions.of(10_000, 100_000),
                4
            );
        var expected =
            "Prim: 315 KiB\n" +
            "|-- this.instance: 48 Bytes\n" +
            "|-- Parent array: 78 KiB\n" +
            "|-- Parent cost array: 78 KiB\n" +
            "|-- Priority queue: 157 KiB\n" +
            "    |-- this.instance: 40 Bytes\n" +
            "    |-- heap: 78 KiB\n" +
            "    |-- costs: 78 KiB\n" +
            "    |-- keys: 1296 Bytes\n" +
            "|-- visited: 1296 Bytes\n";
        assertThat(estimate.render()).isEqualTo(expected);
    }
}
