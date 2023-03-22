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
package org.neo4j.gds.triangle;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class IntersectingTriangleCountFactory<CONFIG extends TriangleCountBaseConfig> extends GraphAlgorithmFactory<IntersectingTriangleCount, CONFIG> {

    private static final String INTERSECTING_TRIANGLE_COUNT_TASK_NAME = IntersectingTriangleCount.class.getSimpleName();

    @Override
    public String taskName() {
        return INTERSECTING_TRIANGLE_COUNT_TASK_NAME;
    }

    @Override
    public IntersectingTriangleCount build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return IntersectingTriangleCount.create(
            graph,
            configuration,
            Pools.DEFAULT,
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
        return Tasks.leaf(INTERSECTING_TRIANGLE_COUNT_TASK_NAME, graph.nodeCount());
    }
}
