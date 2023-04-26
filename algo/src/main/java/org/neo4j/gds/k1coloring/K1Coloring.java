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
package org.neo4j.gds.k1coloring;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.neo4j.gds.mem.BitUtil.ceilDiv;

/**
 * <p>
 * This is a parallel implementation of the K1-Coloring algorithm.
 * The Algorithm will assign a color to every node in the graph, trying to optimize for two objectives:
 * </p>
 * <ul>
 *   <li> given a single node, make sure that every neigbor of that node has a different color </li>
 *   <li> use as little colors as possible </li>
 * </ul>
 *
 * <p>
 * The implementation is a greedy implementation based on:<br>
 * <cite>
 * Çatalyürek, Ümit V., et al.
 * "Graph coloring algorithms for multi-core and massively multithreaded architectures."
 * Parallel Computing 38.10-11 (2012): 576-594.
 * https://arxiv.org/pdf/1205.3809.pdf
 * </cite>
 * </p>
 *
 * <p>
 * The implementation is greedy, so it is not garantied to find an optimal solution, i.e. the coloring can be imperfect
 * and contain more colors as needed.
 * </p>
 */
public class K1Coloring extends Algorithm<HugeLongArray> {

    private final Graph graph;
    private final long nodeCount;
    private final ExecutorService executor;
    private final int minBatchSize;
    private final int concurrency;

    private final long maxIterations;

    private BitSet nodesToColor;
    private HugeLongArray colors;
    private long ranIterations;
    private boolean didConverge;

    private BitSet usedColors;

    public K1Coloring(
        Graph graph,
        long maxIterations,
        int minBatchSize,
        int concurrency,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.minBatchSize = minBatchSize;
        this.concurrency = concurrency;
        this.executor = executor;

        this.nodeCount = graph.nodeCount();
        this.maxIterations = maxIterations;

        this.nodesToColor = new BitSet(nodeCount);

        if (maxIterations <= 0L) {
            throw new IllegalArgumentException("Must iterate at least 1 time");
        }
    }

    public long ranIterations() {
        return ranIterations;
    }

    public boolean didConverge() {
        return didConverge;
    }

    public BitSet usedColors() {
        if (usedColors == null) {
            this.usedColors = new BitSet(nodeCount);
            graph.forEachNode((nodeId) -> {
                    usedColors.set(colors.get(nodeId));
                    return true;
                }
            );
        }
        return usedColors;
    }

    public HugeLongArray colors() {
        return colors;
    }

    @Override
    public HugeLongArray compute() {
        progressTracker.beginSubTask();

        colors = HugeLongArray.newArray(nodeCount);
        colors.setAll((nodeId) -> ColoringStep.INITIAL_FORBIDDEN_COLORS);

        ranIterations = 0L;
        nodesToColor.set(0, nodeCount);

        var currentVolume = nodeCount;
        while (ranIterations < maxIterations && !nodesToColor.isEmpty()) {
            terminationFlag.assertRunning();
            runColoring(currentVolume);

            terminationFlag.assertRunning();
            runValidation(currentVolume);

            ++ranIterations;

            if (ranIterations < maxIterations && !nodesToColor.isEmpty()) {
                currentVolume = nodesToColor.cardinality();
            }
        }

        this.didConverge = ranIterations < maxIterations;

        progressTracker.endSubTask();
        return colors();
    }

    private void runColoring(long volume) {
        progressTracker.beginSubTask(volume);
        long nodeCount = graph.nodeCount();
        long approximateRelationshipCount = ceilDiv(graph.relationshipCount(), nodeCount) * nodesToColor.cardinality();
        long adjustedBatchSize = ParallelUtil.adjustedBatchSize(
            approximateRelationshipCount,
            concurrency,
            minBatchSize,
            Integer.MAX_VALUE
        );

        var steps = PartitionUtils.degreePartitionWithBatchSize(
            new SetBitsIterable(nodesToColor).primitiveLongIterator(),
            graph::degree,
            adjustedBatchSize,
            partition -> new ColoringStep(
                graph.concurrentCopy(),
                colors,
                nodesToColor,
                partition,
                getProgressTracker()
            )
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(steps)
            .executor(executor)
            .run();
        progressTracker.endSubTask();
    }

    private void runValidation(long volume) {
        progressTracker.beginSubTask(volume);
        BitSet nextNodesToColor = new BitSet(nodeCount);

        // The nodesToColor bitset is not thread safe, therefore we have to align the batches to multiples of 64
        List<Partition> partitions = PartitionUtils.numberAlignedPartitioning(concurrency, nodeCount, Long.SIZE);

        List<ValidationStep> steps = partitions.stream().map(partition -> new ValidationStep(
            graph.concurrentCopy(),
            colors,
            nodesToColor,
            nextNodesToColor,
            partition,
            progressTracker
        )).collect(Collectors.toList());

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(steps)
            .executor(executor)
            .run();
        this.nodesToColor = nextNodesToColor;
        progressTracker.endSubTask();
    }
}
