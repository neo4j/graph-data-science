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
package org.neo4j.gds.impl.approxmaxkcut;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicByteArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutConfig;

import java.util.List;

public class ApproxMaxKCutFactory<CONFIG extends ApproxMaxKCutConfig> extends GraphAlgorithmFactory<ApproxMaxKCut, CONFIG> {

    public ApproxMaxKCutFactory() {
        super();
    }

    @Override
    public String taskName() {
        return "ApproxMaxKCut";
    }

    @Override
    public ApproxMaxKCut build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        return new ApproxMaxKCut(graph, Pools.DEFAULT, configuration, progressTracker);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.iterativeFixed(
            taskName(),
            () -> List.of(
                Tasks.leaf("place nodes randomly", graph.nodeCount()),
                searchTask(graph.nodeCount(), config.vnsMaxNeighborhoodOrder())
            ),
            config.iterations()
        );
    }

    private static Task searchTask(long nodeCount, int vnsMaxNeighborhoodOrder) {
        if (vnsMaxNeighborhoodOrder > 0) {
            return Tasks.iterativeOpen(
                "variable neighborhood search",
                () -> List.of(localSearchTask(nodeCount))
            );
        }

        return localSearchTask(nodeCount);
    }

    private static Task localSearchTask(long nodeCount) {
        return Tasks.task(
            "local search",
            Tasks.iterativeOpen(
                "improvement loop",
                () -> List.of(
                    Tasks.leaf("compute node to community weights", nodeCount),
                    Tasks.leaf("swap for local improvements", nodeCount)
                )
            ),
            Tasks.leaf("compute current solution cost", nodeCount)
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(ApproxMaxKCut.class);

        builder.perNode("best solution candidate", HugeByteArray::memoryEstimation);
        builder.perNode("solution workspace", HugeByteArray::memoryEstimation);
        builder.perNodeVector(
            "local search improvement costs cache",
            configuration.k(),
            HugeAtomicDoubleArray::memoryEstimation
        );
        builder.perNode("local search set swap status cache", HugeAtomicByteArray::memoryEstimation);

        if (configuration.vnsMaxNeighborhoodOrder() > 0) {
            builder.perNode("vns neighbor solution candidate", HugeByteArray::memoryEstimation);
        }

        return builder.build();
    }
}
