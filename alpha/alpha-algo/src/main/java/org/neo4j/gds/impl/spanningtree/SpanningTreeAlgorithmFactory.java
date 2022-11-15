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
package org.neo4j.gds.impl.spanningtree;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.mem.MemoryUsage;

public class SpanningTreeAlgorithmFactory<CONFIG extends SpanningTreeBaseConfig> extends GraphAlgorithmFactory<Prim, CONFIG> {

    @Override
    public Prim build(Graph graphOrGraphStore, CONFIG configuration, ProgressTracker progressTracker) {

        return new Prim(
            graphOrGraphStore,
            configuration.objective(),
            graphOrGraphStore.toMappedNodeId(configuration.sourceNode()),
            progressTracker
        );

    }

    @Override
    public String taskName() {
        return "SpanningTree";
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return MemoryEstimations.builder(Prim.class)
            .perNode("Parent array", HugeLongArray::memoryEstimation)
            .perNode("Parent cost array", HugeDoubleArray::memoryEstimation)
            .add("Priority queue", HugeLongPriorityQueue.memoryEstimation())
            .perNode("visited", MemoryUsage::sizeOfBitset)
            .build();
    }
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.leaf(taskName(), graph.relationshipCount());
    }

}
