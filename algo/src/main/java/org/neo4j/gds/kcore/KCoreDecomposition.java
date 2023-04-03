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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicIntArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.ParallelIntPageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class KCoreDecomposition extends Algorithm<KCoreDecompositionResult> {

    private final Graph graph;
    private final int concurrency;

    protected KCoreDecomposition(Graph graph, int concurrency, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
    }

    @Override
    public KCoreDecompositionResult compute() {

        HugeAtomicIntArray currentDegrees = HugeAtomicIntArray.of(
            graph.nodeCount(),
            new ParallelIntPageCreator(concurrency)
        );

        HugeIntArray core = HugeIntArray.newArray(graph.nodeCount());
        int degeneracy = -1;
        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency, v -> currentDegrees.set(v, graph.degree(v)));

        AtomicLong remainingNodes = new AtomicLong(graph.nodeCount());
        AtomicLong nodeIndex = new AtomicLong(0);
        AtomicInteger currentCore = new AtomicInteger(0);

        var tasks = createTasks(currentDegrees, core, nodeIndex, currentCore, remainingNodes);

        while (remainingNodes.get() > 0) {

        }

        return ImmutableKCoreDecompositionResult.of(core, degeneracy);
    }

    private List<KCoreDecompositionTask> createTasks(
        HugeAtomicIntArray currentDegrees,
        HugeIntArray core,
        AtomicLong nodeIndex,
        AtomicInteger currentCore,
        AtomicLong remainingNodes
    ) {
        List<KCoreDecompositionTask> tasks = new ArrayList<>();
        for (int taskId = 0; taskId < concurrency; ++taskId) {
            tasks.add(new KCoreDecompositionTask(
                graph.concurrentCopy(),
                currentDegrees,
                core,
                nodeIndex,
                currentCore,
                remainingNodes,
                progressTracker
            ));
        }
        return tasks;
    }
}
