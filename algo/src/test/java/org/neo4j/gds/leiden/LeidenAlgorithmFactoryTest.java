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
package org.neo4j.gds.leiden;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LeidenAlgorithmFactoryTest {

    @Test
    void shouldProduceProgressTask() {

        var config = LeidenStatsConfigImpl.builder().maxLevels(3).build();

        var graph = GdlFactory.of(" CREATE (a:NODE), (b:NODE) ").build().getUnion();

        var task = new LeidenAlgorithmFactory<>().progressTask(graph, config);
        
        var initialization = Tasks.leaf("Initialization", 2);

        var iteration = Tasks.iterativeDynamic("Iteration", () ->
                List.of(
                    Tasks.leaf("Local Move", 1),
                    Tasks.leaf("Modularity Computation", 2),
                    Tasks.leaf("Refinement", 2),
                    Tasks.leaf("Aggregation", 2)
                ),
            3
        );
        var expectedTask = Tasks.task("Leiden", initialization, iteration);

        assertThat(task.render()).isEqualTo(expectedTask.render());
    }

    @Test
    void shouldEstimateMemory() {
        var config = LeidenStatsConfigImpl.builder().maxLevels(3).build();
        var estimate = new LeidenAlgorithmFactory<>().memoryEstimation(config)
            .estimate(
                GraphDimensions.of(10_000, 100_000),
                4
            );
        var expected =
            "Leiden: [3127 KiB ... 5256 KiB]\n" +
            "|-- this.instance: 96 Bytes\n" +
            "|-- local move communities: 78 KiB\n" +
            "|-- local move node volumes: 78 KiB\n" +
            "|-- local move community volumes: 78 KiB\n" +
            "|-- current communities: 78 KiB\n" +
            "|-- local move phase: 1173 KiB\n" +
            "    |-- this.instance: 48 Bytes\n" +
            "    |-- community weights: 78 KiB\n" +
            "    |-- community volumes: 78 KiB\n" +
            "    |-- global queue: 78 KiB\n" +
            "    |-- global queue bitset: 1328 Bytes\n" +
            "    |-- local move task: 938 KiB\n" +
            "        |-- neighbor communities: 78 KiB\n" +
            "        |-- neighbor weights: 78 KiB\n" +
            "        |-- local queue: 78 KiB\n" +
            "            |-- this.instance: 40 Bytes\n" +
            "            |-- array: 78 KiB\n" +
            "|-- modularity computation: 78 KiB\n" +
            "    |-- this.instance: 16 Bytes\n" +
            "    |-- relationships outside community: 78 KiB\n" +
            "    |-- relationship calculator: 128 Bytes\n" +
            "        |-- this.instance: 32 Bytes\n" +
            "|-- dendogram manager: 78 KiB\n" +
            "    |-- this.instance: 40 Bytes\n" +
            "    |-- dendograms: 78 KiB\n" +
            "|-- refinement phase: 470 KiB\n" +
            "    |-- this.instance: 96 Bytes\n" +
            "    |-- encountered communities: 78 KiB\n" +
            "    |-- encountered community weights: 78 KiB\n" +
            "    |-- next community probabilities: 78 KiB\n" +
            "    |-- merged community volumes: 78 KiB\n" +
            "    |-- relationships between communities: 78 KiB\n" +
            "    |-- refined communities: 78 KiB\n" +
            "    |-- merge tracking bitset: 1296 Bytes\n" +
            "|-- aggregation phase: [700 KiB ... 2830 KiB]\n" +
            "    |-- this.instance: 48 Bytes\n" +
            "    |-- aggregated graph: [544 KiB ... 2674 KiB]\n" +
            "    |-- sorted communities: 78 KiB\n" +
            "    |-- atomic coordination array: 78 KiB\n" +
            "|-- post-aggregation phase: 312 KiB\n" +
            "    |-- next local move communities: 78 KiB\n" +
            "    |-- next local move node volumes: 78 KiB\n" +
            "    |-- next local move community volumes: 78 KiB\n" +
            "    |-- community to node map: 78 KiB\n";
        assertThat(estimate.render()).isEqualTo(expected);
    }

    @Test
    void shouldThrowIfNotUndirected() {
        var graph = GdlFactory.of("(a)-->(b)").build().getUnion();
        var config = LeidenStatsConfigImpl.builder().maxLevels(3).build();
        var leidenFactory = new LeidenAlgorithmFactory<>();
        assertThatThrownBy(() -> {
            leidenFactory.build(
                graph,
                config,
                ProgressTracker.NULL_TRACKER
            );
        }).hasMessageContaining(
            "undirected");
    }
}
