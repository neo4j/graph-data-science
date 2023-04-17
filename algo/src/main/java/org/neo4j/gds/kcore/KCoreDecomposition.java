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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicIntArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.ParallelIntPageCreator;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class KCoreDecomposition extends Algorithm<KCoreDecompositionResult> {

    public static final String KCORE_DESCRIPTION = "It computes the k-core values in a network";
    private final Graph graph;
    private final int concurrency;
    private static final int CHUNK_SIZE = 64;
    private int chunkSize;

   public KCoreDecomposition(Graph graph, int concurrency, ProgressTracker progressTracker) {
       super(progressTracker);
       this.graph = graph;
       this.concurrency = concurrency;
       this.chunkSize = CHUNK_SIZE;
   }

    @TestOnly
    KCoreDecomposition(Graph graph, int concurrency, ProgressTracker progressTracker, int chunkSize) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.chunkSize = chunkSize;
    }

    @Override
    public KCoreDecompositionResult compute() {

        HugeAtomicIntArray currentDegrees = HugeAtomicIntArray.of(
            graph.nodeCount(),
            new ParallelIntPageCreator(concurrency)
        );

        HugeIntArray core = HugeIntArray.newArray(graph.nodeCount());
        int degeneracy = 0;

        AtomicLong remainingNodes = new AtomicLong(graph.nodeCount());

        ParallelUtil.parallelForEachNode(graph.nodeCount(), concurrency,
            v -> {
                int degree = graph.degree(v);
                currentDegrees.set(v, degree);
                if (degree == 0)
                    remainingNodes.decrementAndGet();
            }

        );

        AtomicLong nodeIndex = new AtomicLong(0);
        int scanningDegree = 1;

        var tasks = createTasks(currentDegrees, core, nodeIndex, remainingNodes);

        while (remainingNodes.get() > 0) {

            nodeIndex.set(0L);
            for (var task : tasks) {
                task.setScanningDegree(scanningDegree);
            }

            RunWithConcurrency.builder().tasks(tasks).concurrency(concurrency).run();

            int nextScanningDegree = tasks
                .stream()
                .mapToInt(KCoreDecompositionTask::getSmallestActiveDegree)
                .filter(v -> v > -1)
                .min()
                .orElseThrow();

            if (nextScanningDegree == scanningDegree) {
                degeneracy = scanningDegree;
                RunWithConcurrency.builder().tasks(tasks).concurrency(concurrency).run();
                scanningDegree++;
            } else {
                //this is a minor optimization not in paper:
                // if we do not do any updates this round, let's skip directly to the smallest active degree remaining
                //instead of reaching there eventually by doing scanningDegree+1, then +2, etc.
                // Generally degeneracy is usually small though it shouldn't be super effective.
                scanningDegree = nextScanningDegree;
            }

        }

        return ImmutableKCoreDecompositionResult.of(core, degeneracy);
    }

    private List<KCoreDecompositionTask> createTasks(
        HugeAtomicIntArray currentDegrees,
        HugeIntArray core,
        AtomicLong nodeIndex,
        AtomicLong remainingNodes
    ) {
        List<KCoreDecompositionTask> tasks = new ArrayList<>();
        for (int taskId = 0; taskId < concurrency; ++taskId) {
            tasks.add(new KCoreDecompositionTask(
                graph.concurrentCopy(),
                currentDegrees,
                core,
                nodeIndex,
                remainingNodes,
                chunkSize,
                progressTracker
            ));
        }
        return tasks;
    }
}
