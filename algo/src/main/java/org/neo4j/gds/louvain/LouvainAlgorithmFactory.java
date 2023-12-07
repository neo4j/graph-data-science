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
package org.neo4j.gds.louvain;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationFactory;

import java.util.List;

public class LouvainAlgorithmFactory<CONFIG extends LouvainBaseConfig> extends GraphAlgorithmFactory<Louvain, CONFIG> {
    @Override
    public Louvain build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {

        return new Louvain(
            graph,
            configuration,
            configuration.includeIntermediateCommunities(),
            configuration.maxLevels(),
            configuration.maxIterations(),
            configuration.tolerance(),
            configuration.concurrency(),
            progressTracker,
            DefaultPool.INSTANCE
        );
    }

    @Override
    public String taskName() {
        return "Louvain";
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.iterativeDynamic(
            taskName(),
            () -> List.of(ModularityOptimizationFactory.modularityOptimizationProgressTask(graph, config)),
            config.maxLevels()
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return new LouvainMemoryEstimateDefinition().memoryEstimation(config);
    }
}
