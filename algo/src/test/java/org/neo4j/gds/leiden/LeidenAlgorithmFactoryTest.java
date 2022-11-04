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
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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

}
