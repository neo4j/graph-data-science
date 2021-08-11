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
package org.neo4j.gds.degree;

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class DegreeCentralityFactory<CONFIG extends DegreeCentralityConfig> extends AbstractAlgorithmFactory<DegreeCentrality, CONFIG> {

    @Override
    protected String taskName() {
        return "DegreeCentrality";
    }

    @Override
    protected DegreeCentrality build(
        Graph graph, CONFIG configuration, AllocationTracker tracker, ProgressTracker progressTracker
    ) {
        return new DegreeCentrality(graph, Pools.DEFAULT, configuration, progressTracker, tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        MemoryEstimations.Builder builder = MemoryEstimations.builder(DegreeCentrality.class);
        if (configuration.hasRelationshipWeightProperty()) {
            return builder
                .perNode("degree cache", HugeDoubleArray::memoryEstimation)
                .build();
        }
        return builder.build();
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return degreeCentralityProgressTask(graph);
    }

    @NotNull
    public static Task degreeCentralityProgressTask(Graph graph) {
        return Tasks.leaf("compute", graph.nodeCount());
    }
}

