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
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.ParallelIntPageCreator;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class KCoreDecomposition extends Algorithm<KCoreDecompositionResult> {

    public static final String KCORE_DESCRIPTION = "It computes the k-core values in a network";
    private final Graph graph;
    private final int concurrency;
    private static final int CHUNK_SIZE = 64;
    private int chunkSize;
    static int UNASSIGNED = -1;
    //To find relevant vertices  at each step we need to iterate from 0... nodeCount
    //When only 2% nodes remain in the graph, we can create a smaller array to loop over these ones only
    static double REBUILD_CONSTANT = 0.02;

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
        progressTracker.beginSubTask("KCoreDecomposition");

        HugeAtomicIntArray currentDegrees = HugeAtomicIntArray.of(
            graph.nodeCount(),
            new ParallelIntPageCreator(concurrency)
        );

        HugeIntArray core = HugeIntArray.newArray(graph.nodeCount());
        int degeneracy = 0;

        AtomicLong degreeZeroNodes = new AtomicLong();

        ParallelUtil.parallelForEachNode(
            graph.nodeCount(),
            concurrency,
            TerminationFlag.RUNNING_TRUE,
            v -> {
                int degree = graph.degree(v);
                currentDegrees.set(v, degree);
                int coreInitilization = UNASSIGNED;
                if (degree == 0) {
                    degreeZeroNodes.incrementAndGet();
                    coreInitilization = 0;
                }
                core.set(v, coreInitilization);

            }
        );
        long rebuildLimit = (long) Math.ceil(REBUILD_CONSTANT * graph.nodeCount());
        AtomicLong remainingNodes = new AtomicLong(graph.nodeCount() - degreeZeroNodes.get());
        progressTracker.logProgress(degreeZeroNodes.get());

        AtomicLong nodeIndex = new AtomicLong(0);

        int scanningDegree = 1;

        var tasks = createTasks(currentDegrees, core, nodeIndex, remainingNodes);
        boolean hasRebuild = false;

        while (remainingNodes.get() > 0) {

            if (!hasRebuild && remainingNodes.get() < rebuildLimit) {
                rebuild(tasks, core, remainingNodes.get());
                hasRebuild = true;
            }

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
        progressTracker.endSubTask("KCoreDecomposition");

        return ImmutableKCoreDecompositionResult.of(core, degeneracy);
    }

    private List<KCoreDecompositionTask> createTasks(
        HugeAtomicIntArray currentDegrees,
        HugeIntArray core,
        AtomicLong nodeIndex,
        AtomicLong remainingNodes
    ) {
        List<KCoreDecompositionTask> tasks = new ArrayList<>();
        var nodeProvider = new FullNodeProvider(graph.nodeCount());
        for (int taskId = 0; taskId < concurrency; ++taskId) {
            tasks.add(new KCoreDecompositionTask(
                graph.concurrentCopy(),
                currentDegrees,
                core,
                nodeIndex,
                remainingNodes,
                chunkSize,
                nodeProvider,
                progressTracker
            ));
        }
        return tasks;
    }

    private void rebuild(List<KCoreDecompositionTask> tasks, HugeIntArray core, long numberOfRemainingNodes) {
        HugeLongArray nodeOrder = HugeLongArray.newArray(numberOfRemainingNodes);
        AtomicLong atomicIndex = new AtomicLong(0);
        var rebuildTasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new RebuildTask(partition, atomicIndex, core, nodeOrder),
            Optional.empty()
        );
        RunWithConcurrency.builder().tasks(rebuildTasks).concurrency(concurrency).run();
        var newNodeProvider = new ReducedNodeProvider(nodeOrder, numberOfRemainingNodes);
        for (var task : tasks) {
            task.updateNodeProvider(newNodeProvider);
        }
    }
}
