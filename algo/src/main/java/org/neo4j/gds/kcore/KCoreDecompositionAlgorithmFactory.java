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
package org.neo4j.gds.kcore;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicIntArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class KCoreDecompositionAlgorithmFactory<CONFIG extends KCoreDecompositionBaseConfig> extends GraphAlgorithmFactory<KCoreDecomposition, CONFIG> {
    @Override
    public KCoreDecomposition build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {

        return new KCoreDecomposition(graph, configuration.concurrency(), progressTracker);
    }

    @Override
    public String taskName() {
        return "KCoreDecomposition";
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {

        var builder = MemoryEstimations.builder(KCoreDecomposition.class);
        builder
            .perNode("currentDegrees", HugeAtomicIntArray::memoryEstimation)
            .perNode("cores", HugeIntArray::memoryEstimation)
            .perThread("KCoreDecompositionTask", KCoreDecompositionTask.memoryEstimation());

        builder.perGraphDimension("RebuildTask", ((graphDimensions, concurrency) -> {
            var resizedNodeCount = Math.max(
                1,
                (long) Math.ceil(graphDimensions.nodeCount() * KCoreDecomposition.REBUILD_CONSTANT)
            );
            var rebuildTask = RebuildTask.memoryEstimation(resizedNodeCount);
            var totalRebuildTasks = rebuildTask * concurrency;
            var rebuildArray = HugeIntArray.memoryEstimation(resizedNodeCount);
            return MemoryRange.of(totalRebuildTasks + rebuildArray);

        }));


        return builder.build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.nodeCount());
    }
}
