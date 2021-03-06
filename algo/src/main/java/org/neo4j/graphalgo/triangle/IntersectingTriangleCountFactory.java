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
import org.neo4j.graphalgo.AbstractAlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.ProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;

public class IntersectingTriangleCountFactory<CONFIG extends TriangleCountBaseConfig> extends AbstractAlgorithmFactory<IntersectingTriangleCount, CONFIG> {

    @Override
    protected long taskVolume(Graph graph, CONFIG configuration) {
        return graph.nodeCount();
    }

    @Override
    protected String taskName() {
        return IntersectingTriangleCount.class.getSimpleName();
    }

    @Override
    protected IntersectingTriangleCount build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, ProgressTracker progressTracker
    ) {
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
        return Tasks.leaf("compute", graph.nodeCount());
    }
}
