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
package org.neo4j.gds.beta.k1coloring;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.config.IterationsConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public class K1ColoringFactory<T extends K1ColoringConfig> extends AlgorithmFactory<K1Coloring, T> {

    @Override
    protected String taskName() {
        return "k1Coloring";
    }

    @Override
    protected K1Coloring build(
        Graph graph, T configuration, AllocationTracker tracker, ProgressTracker progressTracker
    ) {
        return new K1Coloring(
            graph,
            configuration.maxIterations(),
            configuration.batchSize(),
            configuration.concurrency(),
            Pools.DEFAULT,
            progressTracker,
            tracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(T config) {
        return MemoryEstimations.builder(K1Coloring.class)
            .perNode("colors", HugeLongArray::memoryEstimation)
            .perNode("nodesToColor", MemoryUsage::sizeOfBitset)
            .perThread("coloring", MemoryEstimations.builder()
                .field("coloringStep", ColoringStep.class)
                .perNode("forbiddenColors", MemoryUsage::sizeOfBitset)
                .build())
            .build();
    }

    @Override
    public Task progressTask(Graph graph, T config) {
        return k1ColoringProgressTask(graph, config);
    }

    public static <T extends BaseConfig & IterationsConfig> Task k1ColoringProgressTask(Graph graph, T config) {
        return Tasks.iterativeDynamic(
            "compute",
            () -> List.of(
                Tasks.leaf("color nodes", graph.nodeCount()),
                Tasks.leaf("validate nodes", graph.nodeCount())
            ),
            config.maxIterations()
        );
    }
}
