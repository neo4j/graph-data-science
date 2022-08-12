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
package org.neo4j.gds.traversal;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.ArrayList;

public class RandomWalkAlgorithmFactory<CONFIG extends RandomWalkBaseConfig> extends GraphAlgorithmFactory<RandomWalk, CONFIG> {
    @Override
    public String taskName() {
        return "RandomWalk";
    }

    @Override
    public RandomWalk build(
        Graph graph,
        RandomWalkBaseConfig configuration,
        ProgressTracker progressTracker
    ) {
        return RandomWalk.create(graph, configuration, progressTracker, Pools.DEFAULT);
    }

    @Override
    public Task progressTask(
        Graph graph, CONFIG config
    ) {
        var tasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            tasks.add(DegreeCentralityFactory.degreeCentralityProgressTask(graph));
        }
        tasks.add(Tasks.leaf("create walks", graph.nodeCount()));

        return Tasks.task(taskName(), tasks);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        var memoryUsagePerWalk = MemoryUsage.sizeOfLongArray(config.walkLength());
        var sizeOfBuffer = MemoryUsage.sizeOfObjectArray(config.walkBufferSize());

        var maxMemoryUsage = sizeOfBuffer + MemoryUsage.sizeOfArray(config.walkBufferSize(), memoryUsagePerWalk);

        return MemoryEstimations.builder(RandomWalk.class.getSimpleName())
            .fixed("random walk buffer", MemoryRange.of(sizeOfBuffer, maxMemoryUsage))
            .build();
    }
}
