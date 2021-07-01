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
package org.neo4j.graphalgo.triangle;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.logging.Log;

public class IntersectingTriangleCountFactory<CONFIG extends TriangleCountBaseConfig> implements AlgorithmFactory<IntersectingTriangleCount, CONFIG> {

    @Override
    public IntersectingTriangleCount build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, Log log,
        ProgressEventTracker eventTracker
    ) {

        ProgressLogger progressLogger = new BatchingProgressLogger(
            log,
            graph.nodeCount(),
            getClass().getSimpleName(),
            configuration.concurrency(),
            eventTracker
        );

        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger);

        return IntersectingTriangleCount.create(
            graph,
            configuration,
            Pools.DEFAULT,
            tracker,
            progressTracker
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return MemoryEstimations
            .builder(IntersectingTriangleCount.class)
            .perNode("triangle-counts", HugeAtomicLongArray::memoryEstimation)
            .build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return triangleCountProgressTask(graph);
    }

    @NotNull
    public static Task triangleCountProgressTask(Graph graph) {
        return Tasks.task(
            "IntersectingTriangleCount",
            Tasks.leaf("compute", graph.nodeCount())
        );
    }
}
