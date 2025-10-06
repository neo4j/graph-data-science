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
package org.neo4j.gds.maxflow;

import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class GlobalRelabeling {
    private final long nodeCount;
    private final HugeLongArray label;
    private final AtomicWorkingSet frontier;
    private final HugeAtomicBitSet vertexIsDiscovered;
    private final long source;
    private final long target;
    private final Concurrency concurrency;
    private final Collection<Runnable> globalRelabelingTasks;

    static GlobalRelabeling createRelabeling(
        FlowGraph flowGraph,
        HugeLongArray label,
        long source,
        long target,
        Concurrency concurrency,
        HugeLongArrayQueue[] threadQueues
    ) {
        var vertexIsDiscovered = HugeAtomicBitSet.create(flowGraph.nodeCount());
        var frontier = new AtomicWorkingSet(flowGraph.nodeCount());

        List<Runnable> globalRelabelingTasks = new ArrayList<>();
        for (int i = 0; i < concurrency.value(); i++) {
            globalRelabelingTasks.add(new GlobalRelabellingBFSTask(flowGraph.concurrentCopy(), frontier, vertexIsDiscovered, label, threadQueues[i]));
        }

        return new GlobalRelabeling(flowGraph.nodeCount(), label, frontier, vertexIsDiscovered, source, target, concurrency, globalRelabelingTasks);
    }

    private GlobalRelabeling(long nodeCount, HugeLongArray label, AtomicWorkingSet frontier, HugeAtomicBitSet vertexIsDiscovered, long source, long target, Concurrency concurrency, Collection<Runnable> globalRelabelingTasks) {
        this.nodeCount = nodeCount;
        this.label = label;
        this.frontier = frontier;
        this.vertexIsDiscovered = vertexIsDiscovered;
        this.source = source;
        this.target = target;
        this.concurrency = concurrency;
        this.globalRelabelingTasks = globalRelabelingTasks;
    }

    public void globalRelabeling() {
        label.setAll((i) -> nodeCount);
        label.set(target, 0L);
        frontier.reset();
        frontier.push(target);
        vertexIsDiscovered.clear();
        vertexIsDiscovered.set(source);
        vertexIsDiscovered.set(target);
        while (!frontier.isEmpty()) {
            RunWithConcurrency.builder().concurrency(concurrency).tasks(globalRelabelingTasks).build().run();
            frontier.reset();
            RunWithConcurrency.builder().concurrency(concurrency).tasks(globalRelabelingTasks).build().run();
        }
        label.set(source, nodeCount);

    }
}
