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
package org.neo4j.gds.impl.approxmaxkcut.localsearch;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.haa.HugeAtomicByteArray;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.core.concurrency.AtomicDouble;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.ParallelBytePageCreator;
import org.neo4j.gds.core.utils.paged.ParallelDoublePageCreator;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutConfig;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

public class LocalSearch {

    private static final double DEFAULT_WEIGHT = 0.0D;

    private final Graph graph;
    private final ApproxMaxKCut.Comparator comparator;
    private final ApproxMaxKCutConfig config;
    private final ExecutorService executor;
    private final WeightTransformer weightTransformer;
    private final HugeAtomicDoubleArray nodeToCommunityWeights;
    private final HugeAtomicByteArray swapStatus;
    private final List<Partition> degreePartition;
    private final ProgressTracker progressTracker;

    public LocalSearch(
        Graph graph,
        ApproxMaxKCut.Comparator comparator,
        ApproxMaxKCutConfig config,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.comparator = comparator;
        this.config = config;
        this.executor = executor;
        this.progressTracker = progressTracker;

        this.degreePartition = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> partition,
            Optional.of(config.minBatchSize())
        );

        // Used to keep track of the costs for swapping a node to another community.
        // TODO: If we had pull-based traversal we could have a |V| sized int array here instead of the |V|*k sized
        //  double array.
        this.nodeToCommunityWeights = HugeAtomicDoubleArray.of(
            graph.nodeCount() * config.k(),
            ParallelDoublePageCreator.passThrough(
                config.concurrency())
        );

        // Used to keep track of whether we can swap a node into another community or not.
        this.swapStatus = HugeAtomicByteArray.of(graph.nodeCount(), new ParallelBytePageCreator(config.concurrency()));

        this.weightTransformer = config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D;
    }


    @FunctionalInterface
    interface WeightTransformer {
        double accept(double weight);
    }

    /*
     * This is a Local Search procedure modified to run more efficiently in parallel. Instead of restarting the while
     * loop whenever anything has changed in the candidate solution we try to continue as long as we can in order to
     * avoid the overhead of rescheduling our tasks on threads and possibly losing hot caches.
     */
    public void compute(
        HugeByteArray candidateSolution,
        AtomicDouble cost,
        AtomicLongArray cardinalities,
        BooleanSupplier running
    ) {
        var change = new AtomicBoolean(true);

        progressTracker.beginSubTask();

        progressTracker.beginSubTask();
        while (change.get() && running.getAsBoolean()) {
            nodeToCommunityWeights.setAll(0.0D);
            var nodeToCommunityWeightTasks = degreePartition.stream()
                .map(partition ->
                    new ComputeNodeToCommunityWeights(
                        graph.concurrentCopy(),
                        config.k(),
                        DEFAULT_WEIGHT,
                        weightTransformer,
                        candidateSolution,
                        nodeToCommunityWeights,
                        partition,
                        progressTracker
                    )
                ).collect(Collectors.toList());
            progressTracker.beginSubTask();
            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(nodeToCommunityWeightTasks)
                .executor(executor)
                .run();
            progressTracker.endSubTask();

            swapStatus.setAll(SwapForLocalImprovements.NodeSwapStatus.UNTOUCHED);
            change.set(false);
            var swapTasks = degreePartition.stream()
                .map(partition ->
                    new SwapForLocalImprovements(
                        graph.concurrentCopy(),
                        config,
                        comparator,
                        candidateSolution,
                        cardinalities,
                        nodeToCommunityWeights,
                        swapStatus,
                        change,
                        partition,
                        progressTracker
                    )
                ).collect(Collectors.toList());
            progressTracker.beginSubTask();
            RunWithConcurrency.builder()
                .concurrency(config.concurrency())
                .tasks(swapTasks)
                .executor(executor)
                .run();
            progressTracker.endSubTask();
        }
        progressTracker.endSubTask();

        cost.set(0);
        var costTasks = degreePartition.stream()
            .map(partition ->
                new ComputeCost(
                    graph.concurrentCopy(),
                    DEFAULT_WEIGHT,
                    weightTransformer,
                    candidateSolution,
                    cost,
                    partition,
                    progressTracker
                )
            ).collect(Collectors.toList());
        progressTracker.beginSubTask();
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(costTasks)
            .executor(executor)
            .run();
        progressTracker.endSubTask();

        progressTracker.endSubTask();
    }
}
