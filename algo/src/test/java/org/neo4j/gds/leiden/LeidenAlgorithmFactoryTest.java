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
            "Leiden: [3127 KiB ... 5256 KiB]" + System.lineSeparator() +
            "|-- this.instance: 96 Bytes" + System.lineSeparator() +
            "|-- local move communities: 78 KiB" + System.lineSeparator() +
            "|-- local move node volumes: 78 KiB" + System.lineSeparator() +
            "|-- local move community volumes: 78 KiB" + System.lineSeparator() +
            "|-- current communities: 78 KiB" + System.lineSeparator() +
            "|-- local move phase: 1173 KiB" + System.lineSeparator() +
            "    |-- this.instance: 48 Bytes" + System.lineSeparator() +
            "    |-- community weights: 78 KiB" + System.lineSeparator() +
            "    |-- community volumes: 78 KiB" + System.lineSeparator() +
            "    |-- global queue: 78 KiB" + System.lineSeparator() +
            "    |-- global queue bitset: 1328 Bytes" + System.lineSeparator() +
            "    |-- local move task: 938 KiB" + System.lineSeparator() +
            "        |-- neighbor communities: 78 KiB" + System.lineSeparator() +
            "        |-- neighbor weights: 78 KiB" + System.lineSeparator() +
            "        |-- local queue: 78 KiB" + System.lineSeparator() +
            "            |-- this.instance: 40 Bytes" + System.lineSeparator() +
            "            |-- array: 78 KiB" + System.lineSeparator() +
            "|-- modularity computation: 78 KiB" + System.lineSeparator() +
            "    |-- this.instance: 16 Bytes" + System.lineSeparator() +
            "    |-- relationships outside community: 78 KiB" + System.lineSeparator() +
            "    |-- relationship calculator: 128 Bytes" + System.lineSeparator() +
            "        |-- this.instance: 32 Bytes" + System.lineSeparator() +
            "|-- dendogram manager: 78 KiB" + System.lineSeparator() +
            "    |-- this.instance: 48 Bytes" + System.lineSeparator() +
            "    |-- dendograms: 78 KiB" + System.lineSeparator() +
            "|-- refinement phase: 470 KiB" + System.lineSeparator() +
            "    |-- this.instance: 96 Bytes" + System.lineSeparator() +
            "    |-- encountered communities: 78 KiB" + System.lineSeparator() +
            "    |-- encountered community weights: 78 KiB" + System.lineSeparator() +
            "    |-- next community probabilities: 78 KiB" + System.lineSeparator() +
            "    |-- merged community volumes: 78 KiB" + System.lineSeparator() +
            "    |-- relationships between communities: 78 KiB" + System.lineSeparator() +
            "    |-- refined communities: 78 KiB" + System.lineSeparator() +
            "    |-- merge tracking bitset: 1296 Bytes" + System.lineSeparator() +
            "|-- aggregation phase: [700 KiB ... 2830 KiB]" + System.lineSeparator() +
            "    |-- this.instance: 48 Bytes" + System.lineSeparator() +
            "    |-- aggregated graph: [544 KiB ... 2674 KiB]" + System.lineSeparator() +
            "    |-- sorted communities: 78 KiB" + System.lineSeparator() +
            "    |-- atomic coordination array: 78 KiB" + System.lineSeparator() +
            "|-- post-aggregation phase: 312 KiB" + System.lineSeparator() +
            "    |-- next local move communities: 78 KiB" + System.lineSeparator() +
            "    |-- next local move node volumes: 78 KiB" + System.lineSeparator() +
            "    |-- next local move community volumes: 78 KiB" + System.lineSeparator() +
            "    |-- community to node map: 78 KiB" + System.lineSeparator();
        assertThat(estimate.render()).isEqualTo(expected);
    }

    @Test
    void shouldThrowIfNotUndirected() {
        var graph = GdlFactory.of("(a)-->(b)").build().getUnion();
        var config = LeidenStatsConfigImpl.builder().maxLevels(3).build();
        var leidenFactory = new LeidenAlgorithmFactory<>();
        assertThatThrownBy(() -> leidenFactory.build(
            graph,
            config,
            ProgressTracker.NULL_TRACKER
        )).hasMessageContaining(
            "undirected");
    }
}
